use std::{
    ops::Deref,
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

use crate::{Counts, Error, State, lock::*, producer::Producer, waiter::*};

/// The consuming side of a shared state channel.
///
/// Consumers have read-only access to the shared value and are notified when
/// a producer modifies it. Cloning a consumer increments the consumer reference
/// count. When the last consumer is dropped, all waiters (e.g. [`Producer::unused`])
/// are notified.
#[derive(Debug)]
pub struct Consumer<T> {
    pub(crate) state: Lock<State<T>>,
    pub(crate) counts: Arc<Counts>,
}

impl<T> Consumer<T> {
    /// Poll the shared state with a closure.
    ///
    /// Calls `f` with a [`ConsumerRef`]. If `f` returns [`Poll::Pending`] and the
    /// channel is still open, registers the waiter for notification. Returns
    /// `Err` if the channel is closed while pending.
    pub fn poll<F, R>(&self, waiter: &Waiter, mut f: F) -> Poll<Result<R, Error>>
    where
        F: FnMut(&ConsumerRef<'_, T>) -> Poll<R>,
    {
        let state = self.state.lock();
        let consumer_state = ConsumerRef { state };

        if let Poll::Ready(res) = f(&consumer_state) {
            return Poll::Ready(Ok(res));
        }

        if let Err(err) = consumer_state.state.closed.clone() {
            return Poll::Ready(Err(err));
        }

        // Re-extract state from consumer_state to register
        let mut state = consumer_state.state;
        waiter.register(&mut state.waiters);

        Poll::Pending
    }

    /// Poll for channel closure, registering the waiter if still open.
    pub fn poll_closed(&self, waiter: &Waiter) -> Poll<Error> {
        let mut state = self.state.lock();
        if let Err(err) = state.closed.clone() {
            return Poll::Ready(err);
        }

        waiter.register(&mut state.waiters);
        Poll::Pending
    }

    /// Wait until the channel is closed, returning the close error.
    pub async fn closed(&self) -> Error {
        wait(move |waiter| self.poll_closed(waiter)).await
    }

    /// Upgrade to a Producer, failing if the state is already closed.
    pub fn produce(&self) -> Result<Producer<T>, Error> {
        // Increment first to prevent the last Producer::drop from
        // closing the state between our check and the return.
        self.counts.producers.fetch_add(1, Ordering::Relaxed);

        {
            let state = self.state.lock();
            if let Err(err) = state.closed.clone() {
                self.counts.producers.fetch_sub(1, Ordering::Relaxed);
                return Err(err);
            }
        }

        Ok(Producer {
            state: self.state.clone(),
            counts: self.counts.clone(),
        })
    }

    /// Returns `true` if both consumers share the same underlying state.
    pub fn is_clone(&self, other: &Self) -> bool {
        self.state.is_clone(&other.state)
    }
}

impl<T> Drop for Consumer<T> {
    fn drop(&mut self) {
        // Atomically decrement and check if we were the last consumer
        let prev = self.counts.consumers.fetch_sub(1, Ordering::AcqRel);
        if prev > 1 {
            return;
        }

        // We were the last consumer, need to wake waiters
        let waiters = {
            let mut state = self.state.lock();
            state.waiters.take()
        };

        waiters.wake();
    }
}

impl<T> Clone for Consumer<T> {
    fn clone(&self) -> Self {
        self.counts.consumers.fetch_add(1, Ordering::Relaxed);

        Self {
            state: self.state.clone(),
            counts: self.counts.clone(),
        }
    }
}

/// A read-only guard over the shared state, obtained from [`Consumer::poll`].
///
/// Derefs to `T` for direct access.
#[derive(Debug)]
pub struct ConsumerRef<'a, T> {
    state: LockGuard<'a, State<T>>,
}

impl<T> Deref for ConsumerRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.state.value
    }
}

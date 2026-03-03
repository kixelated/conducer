use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

use crate::{Counts, Error, State, consumer::Consumer, lock::*, waiter::*, weak::Weak};

/// The producing side of a shared state channel.
///
/// Producers hold mutable access to the shared value. When the state is modified
/// through [`ProducerMut`], all registered consumers are automatically notified.
/// Cloning a producer increments the producer reference count. When the last
/// producer is dropped, the channel closes with [`Error::Dropped`].
#[derive(Debug)]
pub struct Producer<T> {
    pub(crate) state: Lock<State<T>>,
    pub(crate) counts: Arc<Counts>,
}

impl<T: Default> Default for Producer<T> {
    fn default() -> Self {
        Self {
            state: Lock::new(State::default()),
            counts: Arc::new(Counts::default()),
        }
    }
}

impl<T> Producer<T> {
    /// Create a new producer with the given initial value.
    pub fn new(value: T) -> Self {
        Self {
            state: Lock::new(State::new(value)),
            counts: Arc::new(Counts::default()),
        }
    }

    /// Create a new [`Consumer`] that shares this producer's state.
    pub fn consume(&self) -> Consumer<T> {
        self.counts.consumers.fetch_add(1, Ordering::Relaxed);

        Consumer {
            state: self.state.clone(),
            counts: self.counts.clone(),
        }
    }

    /// Close the channel with the given error, notifying all consumers.
    pub fn close(&mut self, err: Error) -> Result<(), Error> {
        self.modify()?.close(err);
        Ok(())
    }

    /// Acquire mutable access to the shared state.
    ///
    /// Returns a [`ProducerMut`] guard that automatically notifies consumers
    /// when dropped if the state was modified.
    pub fn modify(&self) -> Result<ProducerMut<'_, T>, Error> {
        let state = self.state.lock();
        state.closed.clone()?;
        Ok(ProducerMut::new(state))
    }

    /// Poll-based mutable access with waker registration.
    ///
    /// Calls `f` with a [`ProducerMut`] guard. If `f` returns [`Poll::Pending`],
    /// registers the waiter for notification when the state next changes.
    pub fn poll_modify<F, R>(&self, waiter: &Waiter, mut f: F) -> Poll<Result<R, Error>>
    where
        F: FnMut(&mut ProducerMut<'_, T>) -> Poll<R>,
    {
        let mut state = self.modify()?;

        if let Poll::Ready(res) = f(&mut state) {
            return Poll::Ready(Ok(res));
        }

        // Reset modified so the drop doesn't immediately wake the waiter we're about to register.
        state.modified = false;

        // Re-extract state from producer_state to register
        let state = state.state.as_mut().unwrap();
        waiter.register(&mut state.waiters);
        Poll::Pending
    }

    /// Wait until the channel is closed, returning the close error.
    pub async fn closed(&self) -> Error {
        wait(move |waiter| self.poll_closed(waiter)).await
    }

    fn poll_closed(&self, waiter: &Waiter) -> Poll<Error> {
        let mut state = self.state.lock();
        if let Err(err) = state.closed.clone() {
            return Poll::Ready(err);
        }

        waiter.register(&mut state.waiters);
        Poll::Pending
    }

    /// Wait until all consumers have been dropped.
    ///
    /// Returns `Ok(())` when no consumers remain, or `Err` if the channel is closed.
    pub async fn unused(&self) -> Result<(), Error> {
        wait(move |waiter| self.poll_unused(waiter)).await
    }

    fn poll_unused(&self, waiter: &Waiter) -> Poll<Result<(), Error>> {
        if self.counts.consumers.load(Ordering::Relaxed) == 0 {
            return Poll::Ready(Ok(()));
        }

        let mut state = self.state.lock();
        if let Err(err) = state.closed.clone() {
            return Poll::Ready(Err(err));
        }

        waiter.register(&mut state.waiters);

        // Re-check after registration to avoid TOCTOU race where the last
        // consumer drops between the initial check and waiter registration.
        if self.counts.consumers.load(Ordering::Relaxed) == 0 {
            return Poll::Ready(Ok(()));
        }

        Poll::Pending
    }

    /// Get read-only access to the shared state.
    pub fn borrow(&self) -> ProducerRef<'_, T> {
        ProducerRef {
            state: self.state.lock(),
        }
    }

    /// Returns `true` if both producers share the same underlying state.
    pub fn is_clone(&self, other: &Self) -> bool {
        self.state.is_clone(&other.state)
    }

    /// Create a [`Weak`] reference that doesn't affect the producer/consumer ref counts.
    pub fn weak(&self) -> Weak<T> {
        Weak {
            state: self.state.clone(),
            counts: self.counts.clone(),
        }
    }
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        self.counts.producers.fetch_add(1, Ordering::Relaxed);

        Self {
            state: self.state.clone(),
            counts: self.counts.clone(),
        }
    }
}

impl<T> Drop for Producer<T> {
    fn drop(&mut self) {
        // Atomically decrement and check if we were the last producer
        let prev = self.counts.producers.fetch_sub(1, Ordering::AcqRel);
        if prev > 1 {
            return;
        }

        // We were the last producer, need to close
        let waiters = {
            let mut state = self.state.lock();
            if state.closed.is_err() {
                return;
            }

            state.closed = Err(Error::Dropped);
            state.waiters.take()
        };

        waiters.wake();
    }
}

/// A mutable guard over the shared state.
///
/// Derefs to `T` for direct access. Automatically notifies all waiting consumers
/// when dropped if the state was accessed mutably.
#[derive(Debug)]
pub struct ProducerMut<'a, T> {
    // Its an option so we can drop it before notifying consumers.
    pub(crate) state: Option<LockGuard<'a, State<T>>>,
    pub(crate) modified: bool,
}

impl<'a, T> ProducerMut<'a, T> {
    fn new(state: LockGuard<'a, State<T>>) -> Self {
        Self {
            state: Some(state),
            modified: false,
        }
    }

    /// NOTE: This takes self so it's impossible to be in a closed state.
    pub fn close(mut self, err: Error) {
        let state = self.state.as_mut().unwrap();
        // We don't need to check for state.closed because we checked when making ProducerMut
        state.closed = Err(err);
        self.modified = true;
    }
}

impl<T> Deref for ProducerMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.state.as_ref().unwrap().value
    }
}

impl<T> DerefMut for ProducerMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // If we use the &mut then notify on Drop.
        self.modified = true;
        &mut self.state.as_mut().unwrap().value
    }
}

impl<T> Drop for ProducerMut<'_, T> {
    fn drop(&mut self) {
        let mut state = self.state.take().unwrap();

        if !self.modified {
            return;
        }

        // Drain wakers while holding lock, then wake after releasing
        let waiters = state.waiters.take();
        drop(state); // Release Mutex BEFORE waking

        waiters.wake();
    }
}

/// A read-only guard over the shared state.
///
/// Derefs to `T` for direct access. Does not notify consumers when dropped.
pub struct ProducerRef<'a, T> {
    pub(crate) state: LockGuard<'a, State<T>>,
}

impl<T> ProducerRef<'_, T> {
    /// Returns `true` if the channel has been closed.
    pub fn is_closed(&self) -> bool {
        self.state.closed.is_err()
    }
}

impl<T> Deref for ProducerRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.state.value
    }
}

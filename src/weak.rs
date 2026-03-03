use std::{
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

use crate::{
    Counts, State,
    consumer::Consumer,
    lock::*,
    producer::{Producer, ProducerRef},
    waiter::*,
};

/// A weak reference to a Producer/Consumer state.
///
/// Does not affect ref counts, so it won't prevent auto-close when all Producers are dropped.
/// Can be upgraded to a full Producer or Consumer.
#[derive(Debug)]
pub struct Weak<T> {
    pub(crate) state: Lock<State<T>>,
    pub(crate) counts: Arc<Counts>,
}

impl<T> Weak<T> {
    /// Upgrade to a [`Producer`], returning `None` if the channel is already closed.
    pub fn produce(&self) -> Option<Producer<T>> {
        // Increment first to prevent the last Producer::drop from
        // closing the state between our check and the return.
        self.counts.producers.fetch_add(1, Ordering::Relaxed);

        {
            let state = self.state.lock();
            if state.closed {
                self.counts.producers.fetch_sub(1, Ordering::Relaxed);
                return None;
            }
        }

        Some(Producer {
            state: self.state.clone(),
            counts: self.counts.clone(),
        })
    }

    /// Create a new [`Consumer`] that shares this state.
    pub fn consume(&self) -> Consumer<T> {
        self.counts.consumers.fetch_add(1, Ordering::Relaxed);

        Consumer {
            state: self.state.clone(),
            counts: self.counts.clone(),
        }
    }

    /// Get read-only access to the shared state.
    pub fn borrow(&self) -> ProducerRef<'_, T> {
        ProducerRef {
            state: self.state.lock(),
        }
    }

    /// Returns `true` if the channel has been closed.
    pub fn is_closed(&self) -> bool {
        self.state.lock().closed
    }

    /// Wait until all consumers have been dropped.
    ///
    /// Returns `Some(())` when no consumers remain, or `None` if the channel is closed.
    pub async fn unused(&self) -> Option<()> {
        wait(move |waiter| self.poll_unused(waiter)).await
    }

    fn poll_unused(&self, waiter: &Waiter) -> Poll<Option<()>> {
        if self.counts.consumers.load(Ordering::Relaxed) == 0 {
            return Poll::Ready(Some(()));
        }

        let mut state = self.state.lock();
        if state.closed {
            return Poll::Ready(None);
        }

        waiter.register(&mut state.waiters);

        // Re-check after registration to avoid TOCTOU race where the last
        // consumer drops between the initial check and waiter registration.
        if self.counts.consumers.load(Ordering::Relaxed) == 0 {
            return Poll::Ready(Some(()));
        }

        Poll::Pending
    }

    /// Returns `true` if both weak references share the same underlying state.
    pub fn is_clone(&self, other: &Self) -> bool {
        self.state.is_clone(&other.state)
    }
}

impl<T> Clone for Weak<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            counts: self.counts.clone(),
        }
    }
}

//! Producer/consumer shared state with async waker-based notification.
//!
//! This crate provides [`Producer`] and [`Consumer`] types that share state through
//! a mutex-protected value. Producers can modify the state and consumers are
//! automatically notified via async wakers. The channel auto-closes when all
//! producers are dropped.

use std::{
    ops::{Deref, DerefMut},
    sync::atomic::AtomicUsize,
};

mod lock;
mod waiter;

mod consumer;
mod producer;
mod weak;

pub use consumer::{Consumer, ConsumerRef};
pub use producer::{Producer, ProducerMut, ProducerRef};
pub use waiter::{Waiter, wait};
pub use weak::Weak;

/// Errors returned when a channel is no longer usable.
#[derive(thiserror::Error, Debug, Clone)]
pub enum Error {
    /// The channel was explicitly closed by a producer.
    #[error("closed")]
    Closed,

    /// All producers were dropped without explicitly closing.
    #[error("dropped")]
    Dropped,
}

/// A convenience type alias for `Result<T, conducer::Error>`.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub(crate) struct State<T> {
    pub value: T,
    pub waiters: waiter::WaiterList,
    pub closed: std::result::Result<(), Error>,
}

impl<T: Default> Default for State<T> {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

impl<T> State<T> {
    pub fn new(value: T) -> Self {
        Self {
            value,
            closed: Ok(()),
            waiters: waiter::WaiterList::new(),
        }
    }
}

impl<T> Deref for State<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<T> DerefMut for State<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

#[derive(Debug)]
pub(crate) struct Counts {
    pub producers: AtomicUsize,
    pub consumers: AtomicUsize,
}

impl Default for Counts {
    fn default() -> Self {
        Self {
            producers: AtomicUsize::new(1),
            consumers: AtomicUsize::new(0),
        }
    }
}

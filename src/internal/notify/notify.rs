// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The latched notification source used by blocking and async observers.

use std::sync::Arc;

use super::inner::Inner;
use super::lock;
use super::notified::Notified;

/// A latched notification source for blocking and asynchronous observers.
pub(crate) struct Notify {
    /// Shared backing keeps in-flight `Notified` futures valid.
    inner: Arc<Inner>,
}

impl Notify {
    /// Creates a notification source with generation zero.
    ///
    /// # Returns
    ///
    /// A notification source with an empty waiter list.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }

    /// Returns the current generation for read-register-recheck wait protocols.
    ///
    /// # Returns
    ///
    /// The generation currently stored by this notification source.
    #[must_use]
    #[inline]
    pub(crate) fn generation(&self) -> u64 {
        lock(&self.inner.state).generation
    }

    /// Creates a future observing changes after the current generation.
    ///
    /// # Returns
    ///
    /// A future that completes after the next generation change.
    #[must_use]
    #[inline]
    pub(crate) fn notified(&self) -> Notified {
        self.notified_since(self.generation())
    }

    /// Creates a future observing changes after a previously read generation.
    ///
    /// # Parameters
    ///
    /// - `generation`: The generation read before registering the waiter.
    ///
    /// # Returns
    ///
    /// A future that completes once the source advances beyond `generation`.
    #[must_use]
    #[inline]
    pub(crate) fn notified_since(&self, generation: u64) -> Notified {
        Notified {
            inner: Arc::downgrade(&self.inner),
            generation,
            waiter_id: None,
        }
    }

    /// Advances the generation and wakes all current observers outside the
    /// lock.
    pub(crate) fn notify_waiters(&self) {
        let wakers = {
            let mut state = lock(&self.inner.state);
            state.generation = state.generation.wrapping_add(1);
            std::mem::take(&mut state.waiters)
                .into_iter()
                .map(|(_, waker)| waker)
                .collect::<Vec<_>>()
        };
        self.inner.changed.notify_all();
        for waker in wakers {
            waker.wake();
        }
    }

    /// Blocks until the source advances beyond a previously observed
    /// generation.
    ///
    /// The predicate is rechecked after every condition-variable wake to handle
    /// spurious wakeups safely.
    ///
    /// # Parameters
    ///
    /// - `generation`: The generation observed before beginning the wait.
    pub(crate) fn wait_blocking_since(&self, generation: u64) {
        let mut state = lock(&self.inner.state);
        while state.generation == generation {
            state = match self.inner.changed.wait(state) {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }
}

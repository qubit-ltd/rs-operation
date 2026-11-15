// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::Inner;
use super::Notified;
use super::lock;

/// A latched notification source for blocking and asynchronous observers.
pub(crate) struct Notify {
    /// Shared generation, waiter list, and blocking observers for this source.
    pub(in crate::v02::internal::notify) inner: Arc<Inner>,
}

impl Notify {
    /// Creates a notification source with generation zero.
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }

    /// Returns the current generation.
    pub(crate) fn generation(&self) -> u64 {
        lock(&self.inner.state).generation
    }

    /// Creates a future observing changes after the current generation.
    pub(crate) fn notified(&self) -> Notified {
        self.notified_since(self.generation())
    }

    /// Creates a future observing changes after `generation`.
    ///
    /// # Parameters
    ///
    /// - `self`: Notification source whose changes are observed.
    /// - `generation`: Last observed generation; a later generation wakes the
    ///   returned future.
    pub(crate) fn notified_since(&self, generation: u64) -> Notified {
        Notified {
            inner: Arc::downgrade(&self.inner),
            generation,
            waiter_id: None,
        }
    }

    /// Advances the generation and wakes current observers outside the lock.
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

    /// Blocks until the generation differs from `generation`.
    ///
    /// # Parameters
    ///
    /// - `self`: Notification source whose changes are awaited.
    /// - `generation`: Last observed generation.
    pub(crate) fn wait_blocking_since(&self, generation: u64) {
        let mut state = lock(&self.inner.state);
        while state.generation == generation {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

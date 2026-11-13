// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Waker;

use super::RegistryState;
use crate::report::OperationId;

/// Stores the registry state and coordinates notifications for resource
/// changes.
///
/// The state mutex protects slot and dependency data, while the atomic epoch
/// and waker list let asynchronous observers detect and await changes without
/// holding the state lock.
pub(in crate::resource) struct RegistryInner {
    /// Identifies this registry instance for resource handles.
    pub(in crate::resource) id: u64,
    /// Identifies the operation whose resources this registry manages.
    pub(in crate::resource) operation_id: OperationId,
    /// Holds registered slots and their dependency relationships.
    pub(in crate::resource) state: Mutex<RegistryState>,
    /// Counts notifications so observers can detect changes between polls.
    pub(in crate::resource) change_epoch: AtomicU64,
    /// Stores deduplicated task wakers waiting for the next registry change.
    pub(in crate::resource) change_wakers: Mutex<Vec<Waker>>,
}

impl RegistryInner {
    /// Returns the current change counter using acquire ordering.
    ///
    /// # Returns
    ///
    /// The epoch value observed by the atomic load.
    #[must_use]
    #[inline]
    pub(in crate::resource) fn change_epoch(&self) -> u64 {
        self.change_epoch.load(Ordering::Acquire)
    }

    /// Advances the change counter and wakes all tasks waiting on this
    /// registry.
    pub(in crate::resource) fn notify_change(&self) {
        self.change_epoch.fetch_add(1, Ordering::AcqRel);
        let waiters = {
            let mut waiters = self
                .change_wakers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    /// Registers a task waker unless an equivalent waker is already waiting.
    ///
    /// # Parameters
    ///
    /// - `waker`: The task waker to add if no equivalent waker is already
    ///   registered.
    pub(in crate::resource) fn register_change_waker(&self, waker: &Waker) {
        let mut candidate = Some(waker.clone());
        {
            let mut waiters = self
                .change_wakers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !waiters.iter().any(|waiter| waiter.will_wake(waker)) {
                waiters.push(candidate.take().expect("candidate waker is present"));
            }
        }
        drop(candidate);
    }

    /// Removes all registered wakers equivalent to the supplied task waker.
    ///
    /// # Parameters
    ///
    /// - `waker`: The task waker whose equivalent registrations should be
    ///   removed.
    pub(in crate::resource) fn remove_change_waker(&self, waker: &Waker) {
        let removed = {
            let mut waiters = self
                .change_wakers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut retained = Vec::with_capacity(waiters.len());
            let mut removed = Vec::new();
            for waiter in waiters.drain(..) {
                if waiter.will_wake(waker) {
                    removed.push(waiter);
                } else {
                    retained.push(waiter);
                }
            }
            *waiters = retained;
            removed
        };
        drop(removed);
    }
}

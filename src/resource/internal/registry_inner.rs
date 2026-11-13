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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;
    use std::task::Waker;

    use super::RegistryInner;
    use crate::report::OperationId;
    use crate::resource::internal::RegistryState;

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_registry_change_wakers_deduplicate_remove_and_wake_on_change() {
        let registry = RegistryInner {
            id: 1,
            operation_id: OperationId::new(1),
            state: Mutex::new(RegistryState {
                slots: Vec::new(),
                dependencies: BTreeMap::new(),
            }),
            change_epoch: AtomicU64::new(0),
            change_wakers: Mutex::new(Vec::new()),
        };
        let removed_counter = Arc::new(WakeCounter::default());
        let notified_counter = Arc::new(WakeCounter::default());
        let removed_waker = Waker::from(Arc::clone(&removed_counter));
        let notified_waker = Waker::from(Arc::clone(&notified_counter));

        registry.register_change_waker(&removed_waker);
        registry.register_change_waker(&removed_waker);
        registry.register_change_waker(&notified_waker);
        assert_eq!(
            registry
                .change_wakers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            2
        );

        registry.remove_change_waker(&removed_waker);
        assert_eq!(
            registry
                .change_wakers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            1
        );
        registry.notify_change();

        assert_eq!(registry.change_epoch(), 1);
        assert_eq!(removed_counter.0.load(Ordering::SeqCst), 0);
        assert_eq!(notified_counter.0.load(Ordering::SeqCst), 1);
        assert!(
            registry
                .change_wakers
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );
    }
}

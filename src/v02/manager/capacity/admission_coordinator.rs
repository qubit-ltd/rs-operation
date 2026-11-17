// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::task::Waker;

use super::internal::CoordinatorState;
use super::internal::Waiter;

/// Coordinates wakeups after RAII permits return to their pools.
pub(crate) struct AdmissionCoordinator {
    /// Serializes multi-pool acquisition and permit-return notifications.
    pub(in crate::v02::manager::capacity) transaction: Mutex<()>,
    /// Generation and bounded waiter slots protected as one state snapshot.
    pub(in crate::v02::manager::capacity) state: Mutex<CoordinatorState>,
}

impl AdmissionCoordinator {
    /// Creates a wake coordinator with a fixed number of waiter slots.
    ///
    /// # Parameters
    ///
    /// - `waiter_limit`: maximum number of registered task wakers.
    ///
    /// # Returns
    ///
    /// A shared coordinator with empty waiter slots.
    #[must_use]
    pub(crate) fn new(waiter_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            transaction: Mutex::new(()),
            state: Mutex::new(CoordinatorState {
                generation: 0,
                waiters: (0..waiter_limit).map(|_| None).collect(),
            }),
        })
    }
}

impl AdmissionCoordinator {
    /// Wakes every waiter after any coordinated capacity change.
    #[inline]
    pub(crate) fn wake_waiters(&self) {
        self.notify();
    }
}

impl AdmissionCoordinator {
    /// Reads the current generation used to detect missed notifications.
    ///
    /// # Returns
    ///
    /// The generation observed while holding the coordinator state lock.
    #[must_use]
    #[inline]
    pub(in crate::v02::manager::capacity) fn generation(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generation
    }

    /// Registers or refreshes a waiter, waking it immediately on a generation
    /// race.
    ///
    /// # Parameters
    ///
    /// - `id`: stable identity of the waiter registration.
    /// - `observed_generation`: generation observed before attempting
    ///   acquisition.
    /// - `waker`: task waker to store or wake when capacity changes.
    pub(in crate::v02::manager::capacity) fn register(&self, id: u64, observed_generation: u64, waker: &Waker) {
        let replacement = waker.clone();
        let (old_waker, should_wake) = {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.generation != observed_generation {
                (Some(replacement), true)
            } else if let Some(waiter) = state.waiters.iter_mut().flatten().find(|item| item.id == id) {
                if waiter.waker.will_wake(waker) {
                    (Some(replacement), false)
                } else {
                    (Some(std::mem::replace(&mut waiter.waker, replacement)), false)
                }
            } else if let Some(slot) = state.waiters.iter_mut().find(|slot| slot.is_none()) {
                *slot = Some(Waiter { id, waker: replacement });
                (None, false)
            } else {
                // The waiter permit pool bounds this list. If that invariant is
                // broken, waking makes progress without storing an unbounded
                // registration.
                (Some(replacement), true)
            }
        };
        drop(old_waker);
        if should_wake {
            waker.wake_by_ref();
        }
    }

    /// Removes the waiter with `id`, releasing its stored waker.
    ///
    /// # Parameters
    ///
    /// - `id`: identity of the registration to remove.
    pub(in crate::v02::manager::capacity) fn unregister(&self, id: u64) {
        let removed = {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .waiters
                .iter_mut()
                .find(|slot| slot.as_ref().is_some_and(|waiter| waiter.id == id))
                .and_then(Option::take)
        };
        drop(removed);
    }

    /// Advances the generation and wakes every registered task.
    pub(in crate::v02::manager::capacity) fn notify(&self) {
        let waiters = {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.generation = state.generation.wrapping_add(1);
            let mut waiters = Vec::with_capacity(state.waiters.len());
            for slot in &mut state.waiters {
                if let Some(waiter) = slot.take() {
                    waiters.push(waiter);
                }
            }
            waiters
        };
        for waiter in waiters {
            waiter.waker.wake();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Wake;

    use super::*;

    struct ReregisteringWake {
        coordinator: Arc<AdmissionCoordinator>,
        wake_count: AtomicUsize,
        reregistered: std::sync::atomic::AtomicBool,
    }

    impl Wake for ReregisteringWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.wake_count.fetch_add(1, Ordering::SeqCst);
            if !self.reregistered.swap(true, Ordering::SeqCst) {
                let waker = Waker::from(Arc::clone(self));
                let generation = self.coordinator.generation();
                self.coordinator.register(1, generation, &waker);
            }
        }
    }

    #[test]
    fn test_notify_batch_excludes_reregistered_waiter() {
        let coordinator = AdmissionCoordinator::new(1);
        let wake = Arc::new(ReregisteringWake {
            coordinator: Arc::clone(&coordinator),
            wake_count: AtomicUsize::new(0),
            reregistered: std::sync::atomic::AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&wake));
        coordinator.register(1, coordinator.generation(), &waker);

        coordinator.notify();

        assert_eq!(wake.wake_count.load(Ordering::SeqCst), 1);
        assert!(coordinator.state.lock().unwrap().waiters[0].is_some());

        coordinator.notify();

        assert_eq!(wake.wake_count.load(Ordering::SeqCst), 2);
        coordinator.unregister(1);
    }

    #[test]
    fn test_notify_batch_handles_empty_and_multiple_waiters_without_shrinking_slots() {
        let coordinator = AdmissionCoordinator::new(3);
        coordinator.notify();

        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wake_count))));
        for id in 1..=3 {
            coordinator.register(id, coordinator.generation(), &waker);
        }
        coordinator.notify();

        assert_eq!(wake_count.load(Ordering::SeqCst), 3);
        assert_eq!(coordinator.state.lock().unwrap().waiters.len(), 3);
        assert!(
            coordinator
                .state
                .lock()
                .unwrap()
                .waiters
                .iter()
                .all(Option::is_none)
        );
    }

    struct CountWake(Arc<AtomicUsize>);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_notify_batch_excludes_unregistered_waiter() {
        let coordinator = AdmissionCoordinator::new(2);
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wake_count))));
        coordinator.register(1, coordinator.generation(), &waker);
        coordinator.register(2, coordinator.generation(), &waker);

        coordinator.unregister(1);
        coordinator.notify();

        assert_eq!(wake_count.load(Ordering::SeqCst), 1);
        let state = coordinator.state.lock().unwrap();
        assert_eq!(state.waiters.len(), 2);
        assert!(state.waiters.iter().all(Option::is_none));
    }

    #[test]
    fn test_notify_batch_generation_race_wakes_registration_immediately() {
        let coordinator = AdmissionCoordinator::new(1);
        let wake_count = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(CountWake(Arc::clone(&wake_count))));
        let observed_generation = coordinator.generation();
        coordinator.notify();

        coordinator.register(1, observed_generation, &waker);

        assert_eq!(wake_count.load(Ordering::SeqCst), 1);
        assert!(coordinator.state.lock().unwrap().waiters[0].is_none());
    }
}

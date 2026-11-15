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
        {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.generation = state.generation.wrapping_add(1);
        }
        loop {
            let waiter = {
                let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                state.waiters.iter_mut().find_map(Option::take)
            };
            let Some(waiter) = waiter else {
                break;
            };
            waiter.waker.wake();
        }
    }
}

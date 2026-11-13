// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Lock recovery and waiter notification primitives shared by supervisor
//! owners.

use std::sync::Mutex;
use std::task::Waker;

use super::State;

/// Takes every registered waiter waker out of the supervisor state.
///
/// Registrations remain in the waiter list with an empty waker slot so they can
/// be notified again when they register a replacement waker.
///
/// # Parameters
///
/// - `state`: Supervisor state whose registered wakers are taken.
///
/// # Returns
///
/// The wakers that were present in the registrations.
#[must_use]
pub(in crate::supervisor) fn take_waiter_wakers(state: &mut State) -> Vec<Waker> {
    state.waiters.iter_mut().filter_map(|(_, waker)| waker.take()).collect()
}

/// Wakes each supplied waiter after the supervisor state lock has been
/// released.
///
/// # Parameters
///
/// - `waiters`: Wakers removed from supervisor state while its lock was held.
pub(in crate::supervisor) fn wake(waiters: Vec<Waker>) {
    for waker in waiters {
        waker.wake();
    }
}

/// Wakes shutdown observers, discarding their registration identifiers.
///
/// # Parameters
///
/// - `waiters`: Shutdown registration identifiers paired with their wakers.
pub(in crate::supervisor) fn wake_shutdown(waiters: Vec<(u64, Waker)>) {
    for (_, waker) in waiters {
        waker.wake();
    }
}

/// Promotes the next eligible queued lease when capacity is available.
///
/// Stale or already-released queue entries are skipped. The returned wakers
/// belong to the promoted lease and should be notified after releasing state
/// locks.
///
/// # Parameters
///
/// - `state`: Supervisor state containing the active count and admission queue.
/// - `active_limit`: Maximum number of leases allowed to hold active capacity.
///
/// # Returns
///
/// Wakers for the promoted lease, or an empty vector when no lease is promoted.
#[must_use]
pub(in crate::supervisor) fn promote_one(state: &mut State, active_limit: usize) -> Vec<Waker> {
    while state.active < active_limit {
        let Some(next) = state.queued.pop_front() else {
            return Vec::new();
        };
        let Some(next) = next.upgrade() else {
            continue;
        };
        let mut lease = lock(&next);
        if lease.execution_released || !lease.queued {
            continue;
        }
        lease.queued = false;
        lease.active = true;
        state.active += 1;
        return std::mem::take(&mut lease.active_waiters);
    }
    Vec::new()
}

/// Locks a mutex, recovering its inner value if a prior holder panicked.
///
/// # Type Parameters
///
/// - `T`: Type of the value protected by the mutex.
///
/// # Parameters
///
/// - `mutex`: Mutex to acquire.
///
/// # Returns
///
/// A guard for the mutex-protected value.
pub(in crate::supervisor) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Idempotent release paths for root execution and result reservations.

use std::mem;
use std::sync::Arc;
use std::sync::Mutex;

use super::Inner;
use super::LeaseState;
use super::service_group_lifecycle::retry_service_cleanups_after_capacity_change;
use super::util::lock;
use super::util::promote_one;
use super::util::take_waiter_wakers;
use super::util::wake;
use super::util::wake_shutdown;

/// Releases a lease's active execution slot and promotes any queued root.
///
/// The transition is idempotent, removes a queued lease when necessary, and
/// wakes capacity and shutdown waiters after releasing the supervisor lock.
///
/// # Parameters
///
/// - `inner`: Shared supervisor state whose execution capacity is updated.
/// - `lease`: The reservation whose execution capacity is released.
pub(in crate::supervisor) fn release_execution(inner: &Arc<Inner>, lease: &Arc<Mutex<LeaseState>>) {
    let (waiters, shutdown_waiters) = {
        let mut state = lock(&inner.state);
        let mut lease_state = lock(lease);
        if lease_state.execution_released {
            return;
        }
        lease_state.execution_released = true;
        let mut promoted = Vec::new();
        if lease_state.active {
            lease_state.active = false;
            state.active = state.active.saturating_sub(1);
            promoted.extend(promote_one(&mut state, inner.limits.active_roots));
        }
        if lease_state.queued {
            lease_state.queued = false;
            state
                .queued
                .retain(|entry| entry.upgrade().is_some_and(|candidate| !Arc::ptr_eq(&candidate, lease)));
        }
        promoted.extend(take_waiter_wakers(&mut state));
        (promoted, mem::take(&mut state.shutdown_waiters))
    };
    wake(waiters);
    wake_shutdown(shutdown_waiters);
    inner.changed.notify_all();
    retry_service_cleanups_after_capacity_change(inner);
}

/// Releases a lease's reserved result capacity after its result is observed or
/// discarded.
///
/// Does nothing when the result slot was already released; otherwise it updates
/// the shared count and wakes waiters that may now reserve result capacity.
///
/// # Parameters
///
/// - `inner`: Shared supervisor state whose result capacity is updated.
/// - `lease`: The reservation whose result slot is released.
pub(in crate::supervisor) fn release_result(inner: &Arc<Inner>, lease: &Arc<Mutex<LeaseState>>) {
    let waiters = {
        let mut state = lock(&inner.state);
        let mut lease_state = lock(lease);
        if !lease_state.result_reserved {
            return;
        }
        lease_state.result_reserved = false;
        state.results = state.results.saturating_sub(1);
        take_waiter_wakers(&mut state)
    };
    wake(waiters);
}

/// Releases all execution and result capacity held by an untransferred lease.
///
/// Each reservation is cleared at most once. The operation wakes capacity and
/// shutdown waiters and retries service cleanups after execution capacity
/// changes.
///
/// # Parameters
///
/// - `inner`: Shared supervisor state whose reservations are updated.
/// - `lease`: The reservation whose execution and result capacity are released.
pub(in crate::supervisor) fn release_all(inner: &Arc<Inner>, lease: &Arc<Mutex<LeaseState>>) {
    let (waiters, shutdown_waiters) = {
        let mut state = lock(&inner.state);
        let mut lease_state = lock(lease);
        let mut promoted = Vec::new();
        if !lease_state.execution_released {
            lease_state.execution_released = true;
            if lease_state.active {
                lease_state.active = false;
                state.active = state.active.saturating_sub(1);
                promoted.extend(promote_one(&mut state, inner.limits.active_roots));
            }
            if lease_state.queued {
                lease_state.queued = false;
                state
                    .queued
                    .retain(|entry| entry.upgrade().is_some_and(|candidate| !Arc::ptr_eq(&candidate, lease)));
            }
        }
        if lease_state.result_reserved {
            lease_state.result_reserved = false;
            state.results = state.results.saturating_sub(1);
        }
        promoted.extend(take_waiter_wakers(&mut state));
        (promoted, mem::take(&mut state.shutdown_waiters))
    };
    wake(waiters);
    wake_shutdown(shutdown_waiters);
    inner.changed.notify_all();
    retry_service_cleanups_after_capacity_change(inner);
}

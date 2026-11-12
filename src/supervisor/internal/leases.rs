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


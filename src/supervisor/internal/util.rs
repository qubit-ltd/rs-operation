// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Lock recovery and waiter notification primitives shared by supervisor owners.

use std::sync::Mutex;
use std::task::Waker;

use super::LeaseState;
use super::State;

pub(in crate::supervisor) fn take_waiter_wakers(state: &mut State) -> Vec<Waker> {
    state.waiters.iter_mut().filter_map(|(_, waker)| waker.take()).collect()
}

pub(in crate::supervisor) fn wake(waiters: Vec<Waker>) {
    for waker in waiters {
        waker.wake();
    }
}

pub(in crate::supervisor) fn wake_shutdown(waiters: Vec<(u64, Waker)>) {
    for (_, waker) in waiters {
        waker.wake();
    }
}

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

pub(in crate::supervisor) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}


// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use super::CURRENT_POOL_ID;
use super::PoolInner;
use super::lock;
use super::notify_capacity;

/// Runs jobs for a pool worker until shutdown has drained outstanding work.
///
/// The worker records its pool identity for deadlock detection, catches job
/// panics so the thread survives, and updates queue/shutdown accounting.
///
/// # Parameters
///
/// * `inner` is the shared state for the pool this worker serves.
pub(in crate::v02::executor) fn pool_worker(inner: Arc<PoolInner>) {
    CURRENT_POOL_ID.with(|current| current.set(inner.id));
    loop {
        let job = {
            let mut state = lock(&inner.state);
            loop {
                if let Some(job) = state.queue.pop_front() {
                    break Some(job);
                }
                if state.stopping && state.outstanding == 0 {
                    state.exited_workers += 1;
                    inner.changed.notify_all();
                    break None;
                }
                state = inner
                    .work_available
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        };
        let Some(job) = job else { break };
        if panic::catch_unwind(AssertUnwindSafe(job)).is_err() {
            lock(&inner.state).job_panicked = true;
        }
        let mut state = lock(&inner.state);
        state.outstanding -= 1;
        drop(state);
        inner.changed.notify_all();
        inner.work_available.notify_all();
        notify_capacity(&inner.capacity_callback);
    }
    CURRENT_POOL_ID.with(|current| current.set(0));
}

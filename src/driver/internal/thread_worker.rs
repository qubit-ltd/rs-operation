// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::cell::Cell;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::ThreadShared;

static NEXT_EXECUTOR_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static CURRENT_EXECUTOR_ID: Cell<u64> = const { Cell::new(0) };
}

/// Allocate a distinct local identity for each executor instance.
pub fn next_executor_id() -> u64 {
    NEXT_EXECUTOR_ID.fetch_add(1, Ordering::Relaxed)
}

/// Return the executor identity associated with this worker thread, if any.
pub fn current_thread_executor_id() -> u64 {
    CURRENT_EXECUTOR_ID.with(Cell::get)
}

/// Execute queued closures until shutdown drains the pending queue.
pub fn thread_worker_loop(shared: Arc<ThreadShared>) {
    CURRENT_EXECUTOR_ID.with(|current| current.set(shared.executor_id));
    loop {
        let job = {
            let mut state = lock(&shared.state);
            loop {
                if let Some(job) = state.queue.pop_front() {
                    break job;
                }
                if state.closed {
                    return;
                }
                state = shared.ready.wait(state).unwrap_or_else(|error| error.into_inner());
            }
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
    }
}

/// Close the queue and wake every blocked worker.
pub fn close_thread_executor(shared: &ThreadShared) {
    let mut state = lock(&shared.state);
    state.closed = true;
    shared.ready.notify_all();
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

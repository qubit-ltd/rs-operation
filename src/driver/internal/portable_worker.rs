// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::thread;

use super::PortableShared;
#[cfg(test)]
use super::active_worker_marker::ActiveWorkerMarker;
use super::worker_marker::WorkerMarker;

/// Poll portable tasks in bounded FIFO turns without holding the scheduler
/// lock.
///
/// # Parameters
///
/// * `shared` — shared queue and task state for this worker.
///
/// The worker owns this `Arc` for its lifetime.
pub fn portable_worker_loop(shared: Arc<PortableShared>) {
    let _worker_marker = WorkerMarker::enter();
    #[cfg(test)]
    let _active_marker = ActiveWorkerMarker::enter(Arc::clone(&shared));
    loop {
        let mut completed_this_turn = 0;
        while completed_this_turn < shared.polls_per_turn {
            let (id, task) = {
                let mut state = lock(&shared.state);
                loop {
                    if state.closed {
                        return;
                    }
                    if let Some(id) = state.ready.pop_front() {
                        if let Some(task) = state.tasks.get(&id).cloned() {
                            task.queued.store(false, Ordering::Release);
                            break (id, task);
                        }
                        continue;
                    }
                    state = shared.ready.wait(state).unwrap_or_else(|error| error.into_inner());
                }
            };
            let Some(mut future) = lock(&task.future).take() else {
                schedule_portable_task(&shared, id);
                continue;
            };
            let mut context = Context::from_waker(&task.wake);
            let poll = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));
            completed_this_turn += 1;
            match poll {
                Ok(Poll::Ready(())) => {
                    let removed = lock(&shared.state).tasks.remove(&id);
                    drop(removed);
                    // A user future destructor can also panic. Isolate that panic so one
                    // failed task cannot terminate a fixed driver worker.
                    let _ = catch_unwind(AssertUnwindSafe(|| drop(future)));
                }
                Err(payload) => {
                    let removed = lock(&shared.state).tasks.remove(&id);
                    drop(removed);
                    let _ = catch_unwind(AssertUnwindSafe(|| drop(future)));
                    let _ = catch_unwind(AssertUnwindSafe(|| drop(payload)));
                }
                Ok(Poll::Pending) => {
                    *lock(&task.future) = Some(future);
                }
            }
        }
        thread::yield_now();
    }
}

/// Insert a task into the ready queue once while preserving the capacity bound.
///
/// # Parameters
///
/// * `shared` — scheduler state receiving the task notification.
/// * `id` — identifier of the task to schedule; missing or closed tasks are
///   ignored.
pub fn schedule_portable_task(shared: &PortableShared, id: u64) {
    let mut state = lock(&shared.state);
    if state.closed {
        return;
    }
    if let Some(task) = state.tasks.get(&id)
        && !task.queued.swap(true, Ordering::AcqRel)
    {
        state.ready.push_back(id);
        shared.ready.notify_one();
    }
}

/// Stop worker loops; task owners are drained after workers join.
///
/// # Parameters
///
/// * `shared` — scheduler state whose workers should be stopped.
pub fn close_portable(shared: &PortableShared) {
    let mut state = lock(&shared.state);
    state.closed = true;
    shared.ready.notify_all();
}

/// Locks scheduler state, recovering the contained value after a prior panic.
///
/// # Type Parameters
///
/// * `T` — the value protected by the mutex.
///
/// # Parameters
///
/// * `mutex` — synchronization primitive to acquire.
///
/// # Returns
///
/// A guard that releases the mutex when dropped.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

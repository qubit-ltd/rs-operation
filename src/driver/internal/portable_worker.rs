// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::thread;

use super::PortableShared;

/// Poll portable tasks in bounded FIFO turns without holding the scheduler lock.
pub fn portable_worker_loop(shared: Arc<PortableShared>) {
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
                            task.queued
                                .store(false, std::sync::atomic::Ordering::Release);
                            break (id, task);
                        }
                        continue;
                    }
                    state = shared
                        .ready
                        .wait(state)
                        .unwrap_or_else(|error| error.into_inner());
                }
            };
            let Some(mut future) = lock(&task.future).take() else {
                schedule_portable_task(&shared, id);
                continue;
            };
            let mut context = Context::from_waker(&task.wake);
            let poll = future.as_mut().poll(&mut context);
            completed_this_turn += 1;
            if poll.is_ready() {
                let removed = lock(&shared.state).tasks.remove(&id);
                drop(removed);
                drop(future);
            } else {
                *lock(&task.future) = Some(future);
            }
        }
        thread::yield_now();
    }
}

/// Insert a task into the ready queue once while preserving the capacity bound.
pub fn schedule_portable_task(shared: &PortableShared, id: u64) {
    let mut state = lock(&shared.state);
    if state.closed {
        return;
    }
    if let Some(task) = state.tasks.get(&id) {
        if !task.queued.swap(true, std::sync::atomic::Ordering::AcqRel) {
            state.ready.push_back(id);
            shared.ready.notify_one();
        }
    }
}

/// Stop worker loops; task owners are drained after workers join.
pub fn close_portable(shared: &PortableShared) {
    let mut state = lock(&shared.state);
    state.closed = true;
    shared.ready.notify_all();
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

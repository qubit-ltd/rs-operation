// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Waker-backed state and scheduling for one portable executor future.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Wake;

use super::PortableInner;
use super::lock;

/// Future task state shared by its executor queue and waker.
pub(in crate::v02::executor) struct PortableTask {
    /// Weak executor reference that avoids a task-to-executor ownership cycle.
    pub(in crate::v02::executor) executor: Weak<PortableInner>,
    /// Future retained until it resolves or the task is otherwise completed.
    pub(in crate::v02::executor) future: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>>,
    /// Whether a wake has already queued this task for polling.
    pub(in crate::v02::executor) queued: AtomicBool,
    /// Whether polling has completed and the task must no longer be scheduled.
    pub(in crate::v02::executor) completed: AtomicBool,
}

impl Wake for PortableTask {
    /// Schedules a consumed waker's task for polling.
    fn wake(self: Arc<Self>) {
        self.schedule();
    }

    /// Schedules a borrowed waker's task for polling.
    fn wake_by_ref(self: &Arc<Self>) {
        self.schedule();
    }
}

impl PortableTask {
    /// Coalesces wakeups and queues this task if its executor is still alive.
    fn schedule(self: &Arc<Self>) {
        if self.completed.load(Ordering::Acquire) || self.queued.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(inner) = self.executor.upgrade() else {
            self.queued.store(false, Ordering::Release);
            return;
        };
        let mut state = lock(&inner.state);
        if self.completed.load(Ordering::Acquire) {
            self.queued.store(false, Ordering::Release);
        } else {
            state.ready.push_back(Arc::clone(self));
            drop(state);
            inner.ready_available.notify_one();
        }
    }
}

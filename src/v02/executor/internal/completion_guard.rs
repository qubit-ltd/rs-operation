// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Exact-once registry cleanup for completed portable tasks.

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;

use super::PortableInner;
use super::PortableTask;
use super::lock;
use super::notify_capacity;

/// Removes a completed task and releases its capacity even if its future
/// destructor unwinds.
pub(in crate::v02::executor) struct CompletionGuard<'a> {
    /// Executor state whose task registry owns the completed task.
    inner: &'a PortableInner,
    /// Task identity used to remove this task and stale scheduling entries.
    task: &'a PortableTask,
    /// Whether normal completion already performed cleanup.
    finished: bool,
}

impl<'a> CompletionGuard<'a> {
    /// Creates a guard that cleans up `task` unless normal completion finishes.
    ///
    /// # Parameters
    ///
    /// - `inner`: Shared state that tracks task ownership and capacity.
    /// - `task`: The task whose future has returned `Ready`.
    ///
    /// # Returns
    ///
    /// A guard that removes the task during normal completion or unwinding.
    pub(in crate::v02::executor) fn new(inner: &'a PortableInner, task: &'a PortableTask) -> Self {
        Self {
            inner,
            task,
            finished: false,
        }
    }

    /// Cleans up a completed task once and disarms unwind cleanup.
    pub(in crate::v02::executor) fn finish(&mut self) {
        self.finished = true;
        self.cleanup();
    }

    /// Removes the task registration and notifies workers and capacity waiters.
    fn cleanup(&self) {
        let removed = {
            let mut state = lock(&self.inner.state);
            let active_index = state
                .active
                .iter()
                .position(|active| std::ptr::eq(active.as_ref(), self.task));
            active_index.map(|index| {
                let removed = state.active.swap_remove(index);
                state.ready.retain(|queued| !std::ptr::eq(queued.as_ref(), self.task));
                state.tasks -= 1;
                removed
            })
        };
        let removed_active = removed.is_some();
        drop(removed);
        if removed_active {
            self.inner.ready_available.notify_all();
            if std::thread::panicking() {
                let callback = catch_unwind(AssertUnwindSafe(|| {
                    notify_capacity(&self.inner.capacity_callback);
                }));
                drop(callback);
            } else {
                notify_capacity(&self.inner.capacity_callback);
            }
        }
    }
}

impl Drop for CompletionGuard<'_> {
    /// Performs registry cleanup when dropping the completed future unwinds.
    fn drop(&mut self) {
        if !self.finished {
            self.cleanup();
        }
    }
}

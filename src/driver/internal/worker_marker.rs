// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::cell::Cell;

thread_local! { static IN_PORTABLE_WORKER: Cell<bool> = const { Cell::new(false) }; }

/// Reports whether the current thread is executing a portable driver worker.
///
/// # Returns
///
/// `true` when the current thread is inside a portable worker scope.
#[must_use]
pub fn is_portable_worker_thread() -> bool {
    IN_PORTABLE_WORKER.with(Cell::get)
}

/// Marks the current thread as running a portable driver worker for its scope.
#[must_use]
pub(super) struct WorkerMarker;

impl WorkerMarker {
    /// Enters the portable worker scope on the current thread.
    ///
    /// # Returns
    ///
    /// A guard that clears the thread-local marker when dropped.
    pub(super) fn enter() -> Self {
        IN_PORTABLE_WORKER.with(|value| value.set(true));
        Self
    }
}

impl Drop for WorkerMarker {
    /// Clears the worker marker when the portable worker scope ends.
    fn drop(&mut self) {
        IN_PORTABLE_WORKER.with(|value| value.set(false));
    }
}

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::PortableShared;

/// Tracks a worker's lifetime in the test-only active worker count.
#[must_use]
pub(super) struct ActiveWorkerMarker(
    /// Shared scheduler whose active worker count is tracked by this guard.
    Arc<PortableShared>,
);

impl ActiveWorkerMarker {
    /// Increments the shared active worker count for this worker scope.
    ///
    /// # Parameters
    ///
    /// * `shared` — shared test scheduler state whose worker count is tracked.
    ///
    /// # Returns
    ///
    /// A guard that decrements the active count when dropped.
    pub(super) fn enter(shared: Arc<PortableShared>) -> Self {
        shared.active_workers.fetch_add(1, Ordering::AcqRel);
        Self(shared)
    }
}

impl Drop for ActiveWorkerMarker {
    /// Decrements the shared active worker count when the worker exits.
    fn drop(&mut self) {
        self.0.active_workers.fetch_sub(1, Ordering::AcqRel);
    }
}

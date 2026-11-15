// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use super::super::cleanup_pump::CleanupPump;
use super::super::internal::QueueState;
use super::super::lock;
use crate::v02::executor::CleanupExecutor;

/// Shared queue state held by a dispatcher and its cleanup permits.
///
/// The manager must keep this state alive while queued jobs remain. Its
/// destructor defensively preserves pending owners if that invariant is broken.
pub(in crate::v02::resource::cleanup) struct CleanupDispatcherInner {
    /// Maximum outstanding resource reservations and queued jobs.
    pub(in crate::v02::resource::cleanup) capacity: usize,
    /// Executor that accepts jobs removed from the pending queue.
    pub(in crate::v02::resource::cleanup) executor: Arc<CleanupExecutor>,
    /// Admission flag, reservation count and FIFO pending jobs.
    pub(in crate::v02::resource::cleanup) state: Mutex<QueueState>,
    /// Serializes concurrent attempts to drive the pending queue.
    pub(in crate::v02::resource::cleanup) driver: Mutex<()>,
    /// Optional pump that retries this dispatcher when executor capacity opens.
    pub(in crate::v02::resource::cleanup) pump: Option<Arc<CleanupPump>>,
    /// Pump registry identifier, protected while registration changes.
    pub(in crate::v02::resource::cleanup) pump_registration: Mutex<Option<u64>>,
}

impl Drop for CleanupDispatcherInner {
    /// Removes the pump index and preserves pending jobs on premature teardown.
    ///
    /// # Parameters
    ///
    /// - `self`: Shared dispatcher state being destroyed.
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
        for job in state.pending.drain(..) {
            // A dispatcher must be retained by its manager while pending is
            // nonzero. Preserve owners if that lifetime invariant is broken.
            std::mem::forget(job);
        }
        if let (Some(pump), Some(id)) = (&self.pump, *lock(&self.pump_registration)) {
            lock(&pump.inner.state).dispatchers.remove(&id);
        }
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use super::super::CapacityPermit;

/// Releases queue accounting and its capacity permit when a job starts or is
/// dropped.
pub(super) struct QueuedOperationGuard {
    /// Shared queue count decremented when execution starts or is abandoned.
    queued: Arc<AtomicUsize>,
    /// Prevents double decrement when a started job is later dropped.
    started: bool,
    /// Queue slot held until the executor begins the job.
    queue_permit: Option<CapacityPermit>,
}

impl QueuedOperationGuard {
    /// Tracks one accepted job until it starts or is discarded.
    ///
    /// # Parameters
    ///
    /// - `queued`: Shared count of jobs still waiting to start.
    /// - `queue_permit`: Capacity reserved for this queued job.
    pub(super) fn new(queued: Arc<AtomicUsize>, queue_permit: CapacityPermit) -> Self {
        Self {
            queued,
            started: false,
            queue_permit: Some(queue_permit),
        }
    }
    /// Marks the job started and releases its queue permit.
    ///
    /// # Parameters
    ///
    /// - `self`: Guard tracking this queued operation.
    pub(super) fn start(&mut self) {
        if !self.started {
            self.started = true;
            self.queued.fetch_sub(1, Ordering::AcqRel);
            drop(self.queue_permit.take());
        }
    }
}

impl Drop for QueuedOperationGuard {
    /// Removes an abandoned job from the queue count.
    ///
    /// # Parameters
    ///
    /// - `self`: Guard being dropped with the job's started state.
    fn drop(&mut self) {
        if !self.started {
            self.queued.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

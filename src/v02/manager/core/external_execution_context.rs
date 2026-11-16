// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Execution lifecycle state for externally driven operations.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use super::ExecutionFrameGuard;
use super::ExecutionPhase;
use super::OperationCore;
use super::lock;
use crate::v02::scope::ScopeCore;

/// Tracks execution resources for a manager-driven external operation.
pub struct ExternalExecutionContext {
    /// Operation lifecycle state updated when external execution completes.
    operation: Arc<OperationCore>,
    /// Submission lease released when execution is finalized.
    lease: Arc<dyn super::SubmissionLease>,
    /// Scope whose body completion is recorded during finalization.
    scope: Arc<ScopeCore>,
    /// Execution frame retained until the external body finishes.
    frame: Mutex<Option<ExecutionFrameGuard>>,
    /// Ensures finalization effects run only once.
    finalized: AtomicBool,
}

impl ExternalExecutionContext {
    /// Creates lifecycle state for a manager-driven external operation.
    ///
    /// # Parameters
    ///
    /// - `operation`: Operation whose execution state is tracked.
    /// - `lease`: Submission lease held until execution finishes.
    /// - `scope`: Scope whose body completion is recorded.
    /// - `frame`: Optional execution frame retained during the external body.
    pub(super) fn new(
        operation: Arc<OperationCore>,
        lease: Arc<dyn super::SubmissionLease>,
        scope: Arc<ScopeCore>,
        frame: Option<ExecutionFrameGuard>,
    ) -> Self {
        Self {
            operation,
            lease,
            scope,
            frame: Mutex::new(frame),
            finalized: AtomicBool::new(false),
        }
    }

    /// Marks external execution finished and releases its retained resources.
    pub(super) fn finish(&self) {
        if self.finalized.swap(true, Ordering::AcqRel) {
            return;
        }
        self.operation.set_execution(ExecutionPhase::Finished);
        self.lease.execution_finished();
        drop(lock(&self.frame).take());
        self.scope.mark_body_finished();
    }
}

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
use super::super::OperationJob;
use super::super::SubmissionPermit;
use super::queued_operation_future::QueuedOperationFuture;
use super::queued_operation_guard::QueuedOperationGuard;

/// Holds an executor reservation until its matching operation job is
/// dispatched.
pub(super) enum ProductionPermit {
    /// Permit for submitting a blocking closure and tracking its queue slot.
    Sync(
        /// Blocking executor reservation consumed by the synchronous job.
        crate::v02::executor::ThreadPoolPermit,
        /// Queue counter decremented when the job starts or is cancelled.
        Arc<AtomicUsize>,
        /// Capacity permit retained until the job starts.
        CapacityPermit,
    ),
    /// Permit for spawning a future and tracking its queue slot.
    Async(
        /// Portable executor reservation consumed by the async job.
        crate::v02::executor::PortablePermit,
        /// Queue counter decremented when the future starts or is cancelled.
        Arc<AtomicUsize>,
        /// Capacity permit retained until the future starts.
        CapacityPermit,
    ),
}

impl SubmissionPermit for ProductionPermit {
    /// Dispatches the job through its matching executor and releases queue
    /// capacity at start.
    ///
    /// # Parameters
    ///
    /// - `self`: Executor reservation that accepts the matching operation job.
    /// - `job`: The synchronous or asynchronous job matching this permit.
    fn dispatch(self: Box<Self>, job: OperationJob) {
        match (*self, job) {
            (Self::Sync(permit, queued, queue), OperationJob::Sync(job)) => {
                queued.fetch_add(1, Ordering::AcqRel);
                permit.submit(move || {
                    let _started = QueuedOperationGuard::new(queued, queue);
                    job();
                });
            }
            (Self::Async(permit, queued, queue), OperationJob::Async(future)) => {
                queued.fetch_add(1, Ordering::AcqRel);
                permit.spawn(QueuedOperationFuture {
                    future,
                    guard: Some(QueuedOperationGuard::new(queued, queue)),
                });
            }
            _ => unreachable!("ManagerCore dispatch kind matches its reservation"),
        }
    }
}

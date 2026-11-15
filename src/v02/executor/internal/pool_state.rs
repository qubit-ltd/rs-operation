// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Admission, job queue, and worker shutdown state for a blocking pool.

use std::collections::VecDeque;

/// Mutable lifecycle and queue state protected by the pool mutex.
pub(in crate::v02::executor) struct PoolState {
    /// Whether the pool currently accepts new jobs or reservations.
    pub(in crate::v02::executor) accepting: bool,
    /// Whether workers should exit after draining available work.
    pub(in crate::v02::executor) stopping: bool,
    /// Accepted jobs that have not yet completed.
    pub(in crate::v02::executor) outstanding: usize,
    /// Number of workers that have exited after shutdown began.
    pub(in crate::v02::executor) exited_workers: usize,
    /// Whether any worker observed a panic while running a job.
    pub(in crate::v02::executor) job_panicked: bool,
    /// Accepted jobs waiting for a worker to take them.
    pub(in crate::v02::executor) queue: VecDeque<Box<dyn FnOnce() + Send + 'static>>,
}

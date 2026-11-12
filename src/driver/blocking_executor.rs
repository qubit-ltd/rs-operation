// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::RejectedJob;

/// Bounded blocking-work submission interface.
pub trait BlockingExecutor: Send + Sync {
    /// Submit a blocking closure, returning the same closure if rejected.
    ///
    /// `RejectedJob::error` reports queue capacity or shutdown, and
    /// `RejectedJob::job` is the original closure, which has not been invoked.
    fn execute<F>(&self, job: F) -> Result<(), RejectedJob<F>>
    where
        F: FnOnce() + Send + 'static;

    /// Return whether the caller is one of this executor's worker threads.
    fn is_current_thread_worker(&self) -> bool {
        false
    }
}

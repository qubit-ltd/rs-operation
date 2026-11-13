// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::RejectedJob;

/// Bounded blocking-work submission interface.
///
/// # Examples
///
/// ```
/// use qubit_operation::driver::BlockingExecutor;
/// use qubit_operation::driver::ThreadExecutor;
///
/// let executor = ThreadExecutor::new(1, 4).expect("executor starts");
/// assert!(executor.execute(|| {}).is_ok());
/// ```
pub trait BlockingExecutor: Send + Sync {
    /// Submit a blocking closure, returning the same closure if rejected.
    ///
    /// `RejectedJob::error` reports queue capacity or shutdown, and
    /// `RejectedJob::job` is the original closure, which has not been invoked.
    ///
    /// # Type Parameters
    ///
    /// * `F` - A one-shot closure that can be sent to a worker and owns all
    ///   data it needs for a `'static` lifetime.
    ///
    /// # Parameters
    ///
    /// * `job` - The blocking closure to enqueue. It is not invoked when the
    ///   submission is rejected.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the closure is accepted into the bounded queue, or an
    /// error containing the uninvoked closure when it is rejected.
    ///
    /// # Errors
    ///
    /// Returns [`RejectedJob`] when the queue is at capacity or the executor
    /// has shut down; the error retains the original closure for the caller.
    fn execute<F>(&self, job: F) -> Result<(), RejectedJob<F>>
    where
        F: FnOnce() + Send + 'static;

    /// Return whether the caller is one of this executor's worker threads.
    ///
    /// # Returns
    ///
    /// `true` when the caller is a worker belonging to this executor.
    #[must_use]
    #[inline]
    fn is_current_thread_worker(&self) -> bool {
        false
    }
}

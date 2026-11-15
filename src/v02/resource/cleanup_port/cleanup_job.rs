// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::executor::CleanupExecutor;

/// A type-erased cleanup action that owns the resource until it is run.
pub(crate) trait CleanupJob: Send + 'static {
    /// Schedules the owner on its appropriate bounded executor.
    ///
    /// On admission failure, returns the same job intact so the reserved
    /// dispatch slot can keep it pending without dropping the resource.
    ///
    /// # Parameters
    ///
    /// - `self`: Cleanup job whose resource remains owned until accepted.
    /// - `executor`: Bounded executor on which this job attempts to run.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the executor accepts the job. `Err(job)` returns ownership
    /// to the dispatcher when executor admission is unavailable.
    ///
    /// # Errors
    ///
    /// The returned job is the backpressure signal; it preserves the resource
    /// so the dispatcher can retain it and retry later.
    fn schedule(self: Box<Self>, executor: Arc<CleanupExecutor>) -> Result<(), Box<dyn CleanupJob>>;
}

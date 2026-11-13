// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The internal port for submitting synchronous work.

use super::BlockingRejected;

/// Submits a one-shot blocking task to an application or domain executor.
pub(crate) trait BlockingExecutor: Send + Sync {
    /// Runs or schedules the owned task, returning it with an error on
    /// rejection.
    ///
    /// # Parameters
    ///
    /// - `task`: The one-shot blocking operation to execute.
    ///
    /// # Returns
    ///
    /// Returns `()` when the executor accepts the task.
    ///
    /// # Errors
    ///
    /// Returns the rejected task and its failure reason if it is not accepted.
    fn submit(&self, task: Box<dyn FnOnce() + Send + 'static>) -> Result<(), BlockingRejected>;
}

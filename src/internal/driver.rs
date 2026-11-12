// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The internal port for submitting managed asynchronous work.

use super::BoxFuture;
use super::DriverRejected;
use crate::protocol::ManagedOperationControl;

/// Accepts owned work and returns a control handle only after registration.
///
/// Implementations must ensure that the task is registered before execution
/// begins, so the caller can safely coordinate cancellation and completion.
pub(crate) trait Driver: Send + Sync {
    /// Registers and submits a task, returning its operation control handle.
    ///
    /// On rejection, the error retains the unsubmitted task so its ownership
    /// can be recovered by the caller.
    ///
    /// # Parameters
    ///
    /// - `task`: The owned future to register and submit.
    ///
    /// # Returns
    ///
    /// The control handle for a registered task.
    ///
    /// # Errors
    ///
    /// Returns the rejected task and its failure reason if registration fails.
    fn submit(&self, task: BoxFuture<()>) -> Result<Box<dyn ManagedOperationControl>, DriverRejected>;
}

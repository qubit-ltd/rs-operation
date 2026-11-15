// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::OperationJob;

/// Pre-reserved, infallible handoff to an execution engine.
pub(crate) trait SubmissionPermit: Send + 'static {
    /// Transfers one accepted job to its execution engine without further
    /// admission.
    ///
    /// # Parameters
    ///
    /// - `job`: The operation job to dispatch.
    fn dispatch(self: Box<Self>, job: OperationJob);
}

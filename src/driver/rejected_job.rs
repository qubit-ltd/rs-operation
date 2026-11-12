// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::ExecutorError;

/// Rejected submission that retains the original, unstarted closure.
pub struct RejectedJob<F> {
    /// Rejection reason.
    pub error: ExecutorError,
    /// Original job, which has not been invoked.
    pub job: F,
}

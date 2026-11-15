// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::CleanupFailure;
use crate::v02::report::RecoverySnapshot;

/// Outcome of a cleanup retry with its resulting record state.
#[derive(Clone, Debug)]
pub struct RecoveryAttemptReport {
    /// Recovery state after the attempt completed.
    pub recovery: RecoverySnapshot,
    /// Failures produced by this attempt.
    pub failures: Arc<[CleanupFailure]>,
}

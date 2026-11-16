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
    /// Retained failure history at the time this attempt completed. This is a
    /// snapshot of `recovery.last_failures`, not a per-attempt event list; use
    /// `recovery.failures_total` and `recovery.failures_truncated` for the
    /// history count and truncation status.
    pub failures: Arc<[CleanupFailure]>,
}

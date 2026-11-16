// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::CleanupFailure;
use crate::v02::report::RecoveryId;
use crate::v02::report::RecoveryPhase;
use crate::v02::report::ResourceId;

/// Point-in-time recovery state and recent cleanup failures retained while
/// unresolved.
///
/// Obtain this snapshot from
/// [`RecoveryHandle::snapshot`](crate::RecoveryHandle::snapshot).
/// Failed attempts add their failures to `last_failures`, which retains at most
/// the 64 most recent entries in oldest-to-newest order. `failures_total` is a
/// saturating count of all failure events since the last confirmed cleanup,
/// while `failures_truncated` reports whether older entries were evicted.
/// Successful cleanup clears the retained list and resets both metadata
/// fields. Applications that need a complete failure log should emit each
/// failure to their own logging system when observed; the retained history is
/// diagnostic context, not a complete event log.
///
/// # Examples
///
/// ```
/// use qubit_operation::{RecoveryPhase, RecoverySnapshot};
///
/// fn needs_attention(snapshot: &RecoverySnapshot) -> bool {
///     matches!(snapshot.phase, RecoveryPhase::Pending | RecoveryPhase::Unconfirmed)
///         || !snapshot.last_failures.is_empty()
/// }
/// ```
#[derive(Clone, Debug)]
pub struct RecoverySnapshot {
    /// Identity of this recovery record.
    pub id: RecoveryId,
    /// Current cleanup or transfer phase.
    pub phase: RecoveryPhase,
    /// Resources whose cleanup is managed by this record.
    pub resources: Arc<[ResourceId]>,
    /// Number of cleanup attempts started for this record.
    pub attempts: usize,
    /// Total failures since the last confirmed cleanup, saturating at
    /// `usize::MAX`.
    pub failures_total: usize,
    /// Whether `last_failures` omits older failures.
    pub failures_truncated: bool,
    /// The most recent 64 failures, ordered from oldest to newest.
    pub last_failures: Arc<[CleanupFailure]>,
}

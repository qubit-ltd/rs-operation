// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::CleanupFailure;
use crate::v02::report::RecoveryPhase;
use crate::v02::resource::recovery::RecoveryAction;
use crate::v02::resource::recovery::RecoveryFuture;
use crate::v02::resource::recovery::RecoveryPayload;

/// Maximum number of recent cleanup failures retained by one recovery core.
pub(super) const RECOVERY_FAILURE_HISTORY_LIMIT: usize = 64;

/// Mutable phase, payload, action, and failure state for one recovery group.
pub(in crate::v02::resource::recovery) struct State {
    /// Current lifecycle phase of the recovery group.
    pub(super) phase: RecoveryPhase,
    /// Number of retry attempts admitted for this group.
    pub(super) attempts: usize,
    /// Wrapping generation assigned to the current or most recent retry run.
    pub(super) generation: u64,
    /// Owner payload retained while recovery is unresolved, including after
    /// store transfer.
    pub(super) payload: Option<RecoveryPayload>,
    /// Cleanup strategy retained while a retry can still be attempted.
    pub(super) action: Option<Arc<dyn RecoveryAction>>,
    /// Recent cleanup failures, ordered from oldest to newest.
    pub(super) failures: Vec<CleanupFailure>,
    /// Saturating count of all failures since the last confirmed cleanup.
    pub(super) failures_total: usize,
    /// Whether an earlier failure has been evicted from the recent history.
    pub(super) failures_truncated: bool,
    /// A panic made automatic retry unsafe while ownership remains unresolved.
    pub(super) retry_disabled: bool,
    /// Panic payload retained after a retry becomes unconfirmed.
    pub(super) panic: Option<Box<dyn std::any::Any + Send + 'static>>,
    /// Panicked action future retained without polling or dropping it.
    pub(super) unconfirmed_future: Option<RecoveryFuture>,
}

impl State {
    /// Adds a failure and returns any oldest entry evicted to enforce the
    /// limit.
    ///
    /// # Parameters
    ///
    /// * `self` — recovery state whose failure history is updated.
    /// * `failure` — failure event to append.
    ///
    /// # Returns
    ///
    /// The oldest failure evicted to keep the history within its fixed limit.
    pub(super) fn append_failure(&mut self, failure: CleanupFailure) -> Option<CleanupFailure> {
        self.failures_total = self.failures_total.saturating_add(1);
        let evicted = if self.failures.len() == RECOVERY_FAILURE_HISTORY_LIMIT {
            self.failures_truncated = true;
            Some(self.failures.remove(0))
        } else {
            None
        };
        self.failures.push(failure);
        evicted
    }
}

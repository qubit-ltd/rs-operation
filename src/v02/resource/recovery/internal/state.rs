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
    /// Cleanup failures accumulated across retry attempts.
    pub(super) failures: Vec<CleanupFailure>,
    /// A panic made automatic retry unsafe while ownership remains unresolved.
    pub(super) retry_disabled: bool,
    /// Panic payload retained after a retry becomes unconfirmed.
    pub(super) panic: Option<Box<dyn std::any::Any + Send + 'static>>,
    /// Panicked action future retained without polling or dropping it.
    pub(super) unconfirmed_future: Option<RecoveryFuture>,
}

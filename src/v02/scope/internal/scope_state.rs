// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use super::ScopeResourceOwner;
use super::child_record::ChildRecord;
use super::start_state::StartState;
use crate::v02::manager::capacity::PermitBundle;
use crate::v02::report::ChildFailureSummary;
use crate::v02::report::ChildSettlement;
use crate::v02::report::CleanupFailure;
use crate::v02::report::EffectRecord;
use crate::v02::report::OperationId;
use crate::v02::report::RecoveryHandle;
use crate::v02::report::ResourceCleanupReport;
use crate::v02::report::SettlementReport;
use crate::v02::report::TransferReceipt;

/// Mutable lifecycle and bounded diagnostic state guarded by `ScopeCore`.
pub(super) struct ScopeState {
    /// Whether the scope accepts children and resource scopes.
    pub(super) open: bool,
    /// Whether a stop request has begun propagation.
    pub(super) stopping: bool,
    /// Adapter start reservation/commit/activation state.
    pub(super) start: StartState,
    /// Child operations that still owe outcome, settlement, or input release.
    pub(super) active_children: HashMap<OperationId, ChildRecord>,
    /// Child links provisionally reserved while external bind runs.
    pub(super) provisional_children: HashSet<OperationId>,
    /// Bounded retained child settlement details.
    pub(super) children: Vec<ChildSettlement>,
    /// Total observed children, including details omitted after the cap.
    pub(super) children_total: usize,
    /// Whether child details exceeded the diagnostic retention cap.
    pub(super) children_truncated: bool,
    /// First required child failure that determines this scope's outcome.
    pub(super) required_failure: Option<ChildFailureSummary>,
    /// Resource owners ended when this scope closes.
    pub(super) resources: Vec<Box<dyn ScopeResourceOwner>>,
    /// Attached resource scopes whose settlement callbacks have not run.
    pub(super) pending_resource_scopes: usize,
    /// Bounded retained resource cleanup evidence.
    pub(super) resource_reports: Vec<ResourceCleanupReport>,
    /// Total resource reports observed before truncation.
    pub(super) resource_reports_total: usize,
    /// Whether resource report details exceeded the cap.
    pub(super) resource_reports_truncated: bool,
    /// Bounded retained cleanup errors.
    pub(super) cleanup_failures: Vec<CleanupFailure>,
    /// Total cleanup failures observed before truncation.
    pub(super) cleanup_failures_total: usize,
    /// Whether cleanup error details exceeded the cap.
    pub(super) cleanup_failures_truncated: bool,
    /// Bounded retained resource recovery handles.
    pub(super) recoveries: Vec<RecoveryHandle>,
    /// Total recovery handles observed before truncation.
    pub(super) recoveries_total: usize,
    /// Whether recovery details exceeded the cap.
    pub(super) recoveries_truncated: bool,
    /// Bounded effect records retained while their inherited permits are held.
    pub(super) effects: Vec<EffectRecord>,
    /// Permits retaining effect-record quota across all ancestors.
    pub(super) effect_permits: Vec<PermitBundle>,
    /// Number of effect records rejected due to exhausted quota.
    pub(super) effect_overflow: usize,
    /// Bounded retained transfer receipts.
    pub(super) transfers: Vec<TransferReceipt>,
    /// Total transfers observed before truncation.
    pub(super) transfers_total: usize,
    /// Whether transfer details exceeded the cap.
    pub(super) transfers_truncated: bool,
    /// Original settlement evidence returned by the external domain adapter.
    pub(super) external_settlement: Option<Arc<SettlementReport>>,
    /// Callback fired when resource and child settlement are both complete.
    pub(super) settlement_callback: Option<Box<dyn FnOnce() + Send + 'static>>,
    /// Whether the operation body frame has returned or unwound.
    pub(super) body_finished: bool,
    /// Callback fired when the required child outcome set is available.
    pub(super) outcome_callback: Option<Box<dyn FnOnce() + Send + 'static>>,
}

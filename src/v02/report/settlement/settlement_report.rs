// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::ChildSettlement;
use crate::v02::report::CleanupFailure;
use crate::v02::report::EffectRecord;
use crate::v02::report::OperationId;
use crate::v02::report::RecoveryHandle;
use crate::v02::report::ResourceCleanupReport;
use crate::v02::report::SettlementStatus;
use crate::v02::report::TransferReceipt;

/// Resource settlement summary with bounded detail arrays and total counts.
/// For manager-driven external operations, `external_settlement` retains the
/// adapter's domain report. Counts and detail arrays on this report describe
/// the manager scope only; `status` reflects the more severe disposition from
/// either source.
///
/// The detail arrays may be truncated to respect configured memory limits;
/// their corresponding total fields retain the observed counts.
///
/// # Examples
///
/// ```
/// use qubit_operation::SettlementReport;
///
/// // Settlement reports are supplied by operation completion and wait APIs.
/// fn inspect_settlement(report: &SettlementReport) -> bool {
///     let details_kept = report.resource_reports.len();
///     let details_seen = report.resource_reports_total;
///     assert!(details_kept <= details_seen);
///     report.all_released()
/// }
/// ```
#[must_use]
#[derive(Clone, Debug)]
pub struct SettlementReport {
    /// Operation whose resources were settled.
    pub operation_id: OperationId,
    /// Overall disposition of managed resources.
    pub status: SettlementStatus,
    /// Number of resources confirmed released.
    pub released_count: usize,
    /// Number of resources transferred to another owner.
    pub transferred_count: usize,
    /// Number of resources placed in recovery storage.
    pub recovery_count: usize,
    /// Retained child settlement details.
    pub children: Arc<[ChildSettlement]>,
    /// Total child settlements observed before truncation.
    pub children_total: usize,
    /// Whether child details were truncated.
    pub children_truncated: bool,
    /// Retained cleanup failure details.
    pub cleanup_failures: Arc<[CleanupFailure]>,
    /// Total cleanup failures observed before truncation.
    pub cleanup_failures_total: usize,
    /// Whether cleanup failure details were truncated.
    pub cleanup_failures_truncated: bool,
    /// Retained per-resource cleanup evidence.
    pub resource_reports: Arc<[ResourceCleanupReport]>,
    /// Total per-resource reports observed before truncation.
    pub resource_reports_total: usize,
    /// Whether per-resource reports were truncated.
    pub resource_reports_truncated: bool,
    /// Retained resource transfer receipts.
    pub transfers: Arc<[TransferReceipt]>,
    /// Total transfers observed before truncation.
    pub transfers_total: usize,
    /// Whether transfer receipts were truncated.
    pub transfers_truncated: bool,
    /// Retained recovery handles requiring later action.
    pub recoveries: Arc<[RecoveryHandle]>,
    /// Total recoveries observed before truncation.
    pub recoveries_total: usize,
    /// Whether recovery handles were truncated.
    pub recoveries_truncated: bool,
    /// Retained known or uncertain effect records.
    pub effects: Arc<[EffectRecord]>,
    /// Total effect records accepted before truncation.
    pub effects_total: usize,
    /// Effect records that could not be retained because capacity was full.
    pub effect_overflow: usize,
    /// Original domain settlement report supplied by an external adapter.
    /// Scope counts and details remain separate; inspect this report for the
    /// external operation's recovery handles, effects, and other evidence.
    pub external_settlement: Option<Arc<SettlementReport>>,
}

impl SettlementReport {
    /// Reports whether every resource was released without transfer or
    /// recovery.
    ///
    /// # Returns
    ///
    /// `true` when both this scope and any external domain report confirm all
    /// resources released without transfer or recovery.
    #[must_use]
    #[inline]
    pub fn all_released(&self) -> bool {
        self.status == SettlementStatus::Released
            && self.transferred_count == 0
            && self.recovery_count == 0
            && self
                .external_settlement
                .as_ref()
                .is_none_or(|report| report.all_released())
    }
}

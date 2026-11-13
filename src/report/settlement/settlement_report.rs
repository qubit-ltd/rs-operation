// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::super::EffectRecord;
use super::super::OperationId;
use super::super::ResourcePhase;
use super::super::TransferReceipt;
use super::ChildSettlement;
use super::CleanupFailureSummary;

/// Historical settlement snapshot; future recovery actions do not mutate it.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
/// use qubit_operation::report::OperationId;
/// use qubit_operation::report::ResourcePhase;
/// use qubit_operation::report::SettlementReport;
///
/// let report = SettlementReport {
///     operation_id: OperationId::new(8),
///     resource_phase: ResourcePhase::Released,
///     all_released: true,
///     children: Arc::from([]),
///     cleanup_failures: Arc::from([]),
///     transfers: Arc::from([]),
///     unconfirmed_count: 0,
///     effects: Arc::from([]),
///     effect_overflow: 0,
/// };
/// assert!(report.all_released);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SettlementReport {
    /// Operation being settled.
    pub operation_id: OperationId,
    /// Aggregate resource lifecycle phase.
    pub resource_phase: ResourcePhase,
    /// True only when all required release/cleanup responsibilities are
    /// settled.
    pub all_released: bool,
    /// Bounded child settlement facts.
    pub children: Arc<[ChildSettlement]>,
    /// Bounded cleanup failure summaries, without recovery payloads.
    pub cleanup_failures: Arc<[CleanupFailureSummary]>,
    /// Explicitly accepted responsibility transfers.
    pub transfers: Arc<[TransferReceipt]>,
    /// Number of resources whose final release is still unconfirmed.
    pub unconfirmed_count: usize,
    /// Caller-declared side effects retained even when business work was
    /// stopped.
    pub effects: Arc<[EffectRecord]>,
    /// Number of side-effect records omitted after the configured limit was
    /// reached.
    pub effect_overflow: usize,
}

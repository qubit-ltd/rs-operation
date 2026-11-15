// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::OperationId;
use crate::v02::report::SettlementReport;

/// Child result paired with that child's resource settlement.
///
/// Parent settlement reports retain this record so callers can inspect each
/// child's outcome independently from the resources it released.
///
/// # Examples
///
/// Read all child records from a settlement report:
///
/// ```
/// use qubit_operation::ChildSettlement;
/// use qubit_operation::SettlementReport;
///
/// fn child_settlements(report: &SettlementReport) -> &[ChildSettlement] {
///     report.children.as_ref()
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ChildSettlement {
    /// Identity of the child operation.
    pub operation_id: OperationId,
    /// Child execution result summary.
    pub outcome: ChildOutcomeSummary,
    /// Resource disposition reported by the child.
    pub settlement: Arc<SettlementReport>,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::ExecutionPhase;
use crate::v02::report::OperationId;
use crate::v02::report::SettlementReport;
use crate::v02::report::StopReport;

/// Point-in-time child execution, input-release, and resource-settlement facts.
///
/// Managers provide this snapshot through
/// [`crate::v02::protocol::OperationControl::snapshot`]. The optional stop,
/// outcome, and settlement fields distinguish facts that have not yet been
/// published from facts already observed by the parent.
///
/// # Examples
///
/// Obtain a snapshot from the child control port and check whether settlement
/// has been published:
///
/// ```
/// use qubit_operation::OperationControl;
/// use qubit_operation::ChildSnapshot;
///
/// fn child_snapshot(control: &dyn OperationControl) -> ChildSnapshot {
///     control.snapshot()
/// }
///
/// fn has_settlement(snapshot: &ChildSnapshot) -> bool {
///     snapshot.settlement.is_some()
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ChildSnapshot {
    /// Identity of the child operation.
    pub operation_id: OperationId,
    /// Child execution phase.
    pub execution: ExecutionPhase,
    /// Accepted stop report, or `None` if no stop request has been accepted.
    pub stop: Option<StopReport>,
    /// Published child outcome, or `None` while the result is still
    /// unavailable.
    pub outcome: Option<ChildOutcomeSummary>,
    /// Whether the parent's input lease has been released.
    pub input_released: bool,
    /// Final child resource report, or `None` until settlement is published.
    pub settlement: Option<Arc<SettlementReport>>,
}

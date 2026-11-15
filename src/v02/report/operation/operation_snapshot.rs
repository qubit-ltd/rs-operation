// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::ExecutionPhase;
use crate::v02::report::OperationId;
use crate::v02::report::OutcomeAvailability;
use crate::v02::report::ResourceProgress;
use crate::v02::report::SettlementReport;
use crate::v02::report::StopReport;

/// Snapshot separating execution, result availability, and cleanup progress.
///
/// # Examples
///
/// ```
/// use qubit_operation::{OperationObserver, OperationSnapshot};
///
/// fn inspect(observer: &OperationObserver) -> OperationSnapshot {
///     // Result consumption and resource settlement are independent states.
///     observer.snapshot()
/// }
/// ```
#[must_use]
#[derive(Clone, Debug)]
pub struct OperationSnapshot {
    /// Identity of the operation being observed.
    pub operation_id: OperationId,
    /// Whether the body is queued, running, or finished.
    pub execution: ExecutionPhase,
    /// Whether its result is pending, available, taken, or discarded.
    pub outcome: OutcomeAvailability,
    /// Accepted stop request, if one exists.
    pub stop: Option<StopReport>,
    /// Current resource cleanup progress, when known.
    pub resources: Option<ResourceProgress>,
    /// Final resource settlement, when available.
    pub settlement: Option<Arc<SettlementReport>>,
}

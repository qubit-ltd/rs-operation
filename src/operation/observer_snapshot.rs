// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Non-consuming state captured from an operation observer.

use crate::report::ResourcePhase;
use crate::report::SettlementReport;

/// Non-consuming operation state available at an observation deadline.
///
/// Obtain a snapshot from [`super::OperationObserver::observe_until`]; the
/// method returns it together with an observer that remains live afterward.
///
/// # Examples
///
/// ```
/// use qubit_operation::context::Deadline;
/// use qubit_operation::operation::OperationObserver;
/// use qubit_operation::report::ObservationError;
/// async fn inspect(
///     observer: &OperationObserver<(), ()>,
///     deadline: Deadline,
/// ) -> Result<bool, ObservationError> {
///     let observation = observer.observe_until(deadline).await?;
///     Ok(observation.snapshot.business_result_ready)
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ObserverSnapshot {
    /// Current state of managed resources.
    pub resource_phase: ResourcePhase,
    /// Whether the business outcome is available or already resolved.
    pub business_result_ready: bool,
    /// Whether a consumer has taken the typed outcome.
    pub result_taken: bool,
    /// Whether typed result delivery was discarded.
    pub result_discarded: bool,
    /// Final settlement report, if cleanup has completed.
    pub settlement: Option<SettlementReport>,
}

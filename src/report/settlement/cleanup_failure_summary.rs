// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::super::DiagnosticSummary;
use super::super::ResourceId;
use super::CleanupStage;

/// Copyable cleanup failure fact that excludes typed sources and owner
/// payloads.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::CleanupFailureSummary;
/// use qubit_operation::report::CleanupStage;
/// use qubit_operation::report::DiagnosticSummary;
/// use qubit_operation::report::OperationId;
/// use qubit_operation::report::ResourceId;
///
/// let summary = CleanupFailureSummary {
///     resource_id: ResourceId::new(OperationId::new(7), 2, 3),
///     stage: CleanupStage::Removing,
///     summary: DiagnosticSummary::new("removal failed"),
/// };
/// assert_eq!(summary.stage, CleanupStage::Removing);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupFailureSummary {
    /// Resource whose cleanup failed.
    pub resource_id: ResourceId,
    /// Cleanup stage that failed.
    pub stage: CleanupStage,
    /// Sanitized bounded summary.
    pub summary: DiagnosticSummary,
}

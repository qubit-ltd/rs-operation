// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::ChildBusinessOutcome;
use crate::report::DiagnosticSummary;
use crate::report::OperationId;

/// Repeatable child business fact that does not consume its typed result.
///
/// A scope join returns these facts in [`crate::scope::ScopeJoinReport`]'s
/// `required_failures` field.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::ChildBusinessOutcome;
/// use qubit_operation::scope::ScopeJoinReport;
///
/// fn inspect_required_failures(report: &ScopeJoinReport) {
///     for child in &report.required_failures {
///         assert!(matches!(
///             child.outcome,
///             ChildBusinessOutcome::Failed
///                 | ChildBusinessOutcome::Stopped
///                 | ChildBusinessOutcome::Panicked
///         ));
///     }
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildBusinessSummary {
    /// Child that produced this fact.
    pub operation_id: OperationId,
    /// Stable business outcome category.
    pub outcome: ChildBusinessOutcome,
    /// Sanitized, bounded diagnostic summary.
    pub summary: DiagnosticSummary,
}

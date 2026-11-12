// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::DiagnosticSummary;
use super::OperationId;

/// Bounded identity and summary for an unhandled child failure.
///
/// This value is provided by [`OperationOutcome::ChildFailed`] when a required
/// child failure is not handled by its parent operation.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::OperationOutcome;
///
/// fn inspect_failure<E>(outcome: &OperationOutcome<(), E>) {
///     if let OperationOutcome::ChildFailed(failure) = outcome {
///         assert_ne!(failure.operation_id.get(), 0);
///         assert!(!failure.summary.as_str().is_empty());
///     }
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildFailureSummary {
    /// Child that failed.
    pub operation_id: OperationId,
    /// Sanitized, bounded diagnostic summary.
    pub summary: DiagnosticSummary,
}

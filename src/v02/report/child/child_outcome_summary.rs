// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::ChildOutcomeKind;
use crate::v02::report::DiagnosticSummary;
use crate::v02::report::OperationId;

/// Compact outcome information retained for a child operation.
///
/// A parent can inspect this summary when its operation outcome contains a
/// [`crate::v02::report::OperationOutcome::ChildFailed`] payload.
///
/// # Examples
///
/// Extract the child summary from an outcome returned by an operation handle:
///
/// ```
/// use qubit_operation::OperationOutcome;
/// use qubit_operation::ChildOutcomeSummary;
///
/// fn child_summary(outcome: &OperationOutcome<(), String>) -> Option<&ChildOutcomeSummary> {
///     match outcome {
///         OperationOutcome::ChildFailed(failure) => Some(&failure.outcome),
///         _ => None,
///     }
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildOutcomeSummary {
    /// Identity of the child operation.
    pub operation_id: OperationId,
    /// Broad classification of the child's result.
    pub kind: ChildOutcomeKind,
    /// Bounded diagnostic text associated with the result.
    pub summary: DiagnosticSummary,
}

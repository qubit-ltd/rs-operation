// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::OperationId;

/// Associates a failed child identity with its summarized outcome.
///
/// A manager returns this summary as part of
/// [`crate::v02::report::OperationOutcome::ChildFailed`]
/// when a required child prevents its parent from succeeding.
///
/// # Examples
///
/// Read a child failure from an outcome returned by an operation handle:
///
/// ```
/// use qubit_operation::OperationOutcome;
/// use qubit_operation::ChildFailureSummary;
///
/// fn child_failure(outcome: &OperationOutcome<(), String>) -> Option<&ChildFailureSummary> {
///     match outcome {
///         OperationOutcome::ChildFailed(failure) => Some(failure),
///         _ => None,
///     }
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildFailureSummary {
    /// Identity of the child that caused the failure.
    pub operation_id: OperationId,
    /// Summary retained from that child's outcome.
    pub outcome: ChildOutcomeSummary,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::ChildFailureSummary;
use super::PanicSummary;
use super::StopReport;

/// Typed business result, kept separate from resource settlement.
///
/// # Type Parameters
///
/// * `T` is the value returned by a successful operation body.
/// * `E` is the original domain error returned by a failed operation body.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::OperationOutcome;
///
/// fn outcome_kind<T, E>(outcome: OperationOutcome<T, E>) -> &'static str {
///     match outcome {
///         OperationOutcome::Succeeded(_) => "succeeded",
///         OperationOutcome::Failed(_) => "failed",
///         OperationOutcome::ChildFailed(_) => "child failed",
///         OperationOutcome::Stopped(_) => "stopped",
///         OperationOutcome::Panicked(_) => "panicked",
///     }
/// }
///
/// let outcome = OperationOutcome::<u32, &'static str>::Succeeded(42);
/// assert_eq!(outcome_kind(outcome), "succeeded");
/// ```
#[must_use = "operation outcomes must be inspected"]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OperationOutcome<T, E> {
    /// The body and required children completed successfully.
    Succeeded(
        /// The value produced by the operation body after required children
        /// have completed successfully.
        T,
    ),
    /// Preserve the original domain error without requiring `Clone`.
    Failed(
        /// The original domain error returned by the operation body, preserved
        /// without requiring the error type to implement `Clone`.
        E,
    ),
    /// An unhandled required child failed with a different error type.
    ChildFailed(
        /// Bounded identity and diagnostic summary for the required child that
        /// failed without being handled by its parent.
        ChildFailureSummary,
    ),
    /// Delivery stopped before a business result was determined.
    Stopped(
        /// The accepted stop reason and optional acceptance timestamp.
        StopReport,
    ),
    /// The body panicked; payload details are deliberately omitted.
    Panicked(
        /// Sanitized operation and lifecycle-phase details, excluding the
        /// arbitrary panic payload.
        PanicSummary,
    ),
}

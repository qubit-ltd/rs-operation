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
    Succeeded(T),
    /// Preserve the original domain error without requiring `Clone`.
    Failed(E),
    /// An unhandled required child failed with a different error type.
    ChildFailed(ChildFailureSummary),
    /// Delivery stopped before a business result was determined.
    Stopped(StopReport),
    /// The body panicked; payload details are deliberately omitted.
    Panicked(PanicSummary),
}

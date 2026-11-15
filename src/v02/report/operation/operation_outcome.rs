// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::ChildFailureSummary;
use crate::v02::report::ExecutionFailure;
use crate::v02::report::PanicSummary;
use crate::v02::report::StopReport;

/// Result of running an operation body, separate from resource settlement.
///
/// # Type Parameters
///
/// `T` is the success value returned by the body; `E` is its application error.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationOutcome;
///
/// fn take_success<T, E>(outcome: OperationOutcome<T, E>) -> Option<T> {
///     match outcome {
///         OperationOutcome::Succeeded(value) => Some(value),
///         _ => None,
///     }
/// }
/// assert_eq!(take_success(OperationOutcome::<_, ()>::Succeeded(5)), Some(5));
/// ```
#[must_use]
#[derive(Clone, Debug)]
pub enum OperationOutcome<T, E> {
    /// The operation returned its success value.
    Succeeded(
        /// Value returned by the operation body.
        T,
    ),
    /// The operation returned its application error.
    Failed(
        /// Application error returned by the operation body.
        E,
    ),
    /// A required child failure determined the result.
    ChildFailed(
        /// Summary of the required child failure.
        ChildFailureSummary,
    ),
    /// Execution stopped before producing an ordinary result.
    Stopped(
        /// Stop details recorded for the operation.
        StopReport,
    ),
    /// The operation panicked while executing.
    Panicked(
        /// Summary captured from the panic payload.
        PanicSummary,
    ),
    /// The executor failed to execute the submitted body.
    ExecutionFailed(
        /// Failure reported by the executor.
        ExecutionFailure,
    ),
}

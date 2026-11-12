// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::DiagnosticSummary;
use super::OperationId;

/// Sanitized panic diagnostic without arbitrary panic payload disclosure.
///
/// A panic summary is supplied by [`crate::report::OperationOutcome::Panicked`]
/// so callers can identify the affected operation and lifecycle phase without
/// exposing the panic payload.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::OperationOutcome;
///
/// fn inspect_panic<T, E>(outcome: &OperationOutcome<T, E>) {
///     if let OperationOutcome::Panicked(summary) = outcome {
///         assert_ne!(summary.operation_id.get(), 0);
///         assert!(!summary.phase.as_str().is_empty());
///     }
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PanicSummary {
    /// Operation whose body panicked.
    pub operation_id: OperationId,
    /// Bounded lifecycle phase in which panic occurred.
    pub phase: DiagnosticSummary,
}

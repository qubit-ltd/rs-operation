// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::DiagnosticSummary;
use crate::v02::report::ExecutionPhase;
use crate::v02::report::OperationId;

/// Captures panic location in the operation lifecycle and bounded diagnostics.
///
/// # Examples
///
/// Read the operation, phase, and bounded message from a panic report:
///
/// ```
/// use qubit_operation::{ExecutionPhase, OperationId, PanicSummary};
///
/// fn panic_details(summary: &PanicSummary) -> (OperationId, ExecutionPhase, &str) {
///     (summary.operation_id, summary.phase, summary.summary.as_str())
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PanicSummary {
    /// Identity of the operation that panicked.
    pub operation_id: OperationId,
    /// Lifecycle phase in which the panic occurred.
    pub phase: ExecutionPhase,
    /// Bounded panic message or fallback diagnostic.
    pub summary: DiagnosticSummary,
}

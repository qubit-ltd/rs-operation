// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::super::DiagnosticSummary;
use super::super::OperationId;

/// Stable child settlement fact, separate from its business result.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::ChildSettlement;
/// use qubit_operation::report::DiagnosticSummary;
/// use qubit_operation::report::OperationId;
///
/// let child = ChildSettlement {
///     operation_id: OperationId::new(8),
///     released: true,
///     summary: DiagnosticSummary::new("released"),
/// };
/// assert!(child.released);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildSettlement {
    /// Child operation identity.
    pub operation_id: OperationId,
    /// Whether the domain confirmed that the child released its resources.
    pub released: bool,
    /// Sanitized bounded diagnostic summary.
    pub summary: DiagnosticSummary,
}

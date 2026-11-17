// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::DiagnosticSummary;
use crate::v02::report::OperationId;
use crate::v02::report::diagnostics::ExternalProtocolFaultKind;
use crate::v02::report::diagnostics::ExternalProtocolPort;

/// Bounded diagnostic describing a failed external protocol port.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalProtocolFault {
    /// Identity of the operation that owns the protocol.
    pub operation_id: OperationId,
    /// Protocol port that failed.
    pub port: ExternalProtocolPort,
    /// Failure category.
    pub kind: ExternalProtocolFaultKind,
    /// Identity returned by a port when it did not match `operation_id`.
    pub observed_operation_id: Option<OperationId>,
    /// Bounded human-readable summary.
    pub summary: DiagnosticSummary,
}

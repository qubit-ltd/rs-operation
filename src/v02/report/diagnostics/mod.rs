// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod diagnostic_summary;
pub use diagnostic_summary::DiagnosticSummary;

mod panic_summary;
pub use panic_summary::PanicSummary;

mod execution_failure;
pub use execution_failure::ExecutionFailure;

mod external_protocol_port;
pub use external_protocol_port::ExternalProtocolPort;

mod external_protocol_fault_kind;
pub use external_protocol_fault_kind::ExternalProtocolFaultKind;

mod external_protocol_fault;
pub use external_protocol_fault::ExternalProtocolFault;

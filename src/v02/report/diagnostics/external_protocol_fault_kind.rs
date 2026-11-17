// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Kind of failure reported by an external protocol port.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalProtocolFaultKind {
    /// Polling the protocol port panicked.
    Panicked,
    /// The port returned a value for a different operation identity.
    IdentityMismatch,
}

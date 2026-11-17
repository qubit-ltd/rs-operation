// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// External protocol port that reported a fault.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalProtocolPort {
    /// The input release protocol.
    Release,
    /// The external settlement protocol.
    Settlement,
}

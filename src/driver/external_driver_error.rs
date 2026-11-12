// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Invalid transition while reporting external driver lifecycle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExternalDriverError {
    /// The lease has already been marked lost or released.
    AlreadySettled,
}

impl std::fmt::Display for ExternalDriverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("external driver lease has already been settled")
    }
}

impl std::error::Error for ExternalDriverError {}

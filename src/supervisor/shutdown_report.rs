// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Pending work snapshot returned by shutdown observers.

/// Snapshot returned when waiting for service-group work to finish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShutdownReport {
    /// Number of group requests still holding an admission lease.
    pub pending_requests: usize,
    /// Recovery owners still retained by accepted operations.
    pub pending_recovery: usize,
    /// Service resource scopes still awaiting cleanup confirmation.
    pub pending_resources: usize,
}

impl ShutdownReport {
    pub fn is_complete(&self) -> bool {
        self.pending_requests == 0 && self.pending_recovery == 0 && self.pending_resources == 0
    }
}

impl ShutdownReport {
    pub fn is_complete(&self) -> bool {
        self.pending_requests == 0 && self.pending_recovery == 0 && self.pending_resources == 0
    }
}


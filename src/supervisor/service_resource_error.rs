// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Rejected service resource registration retaining its exact owner.

use std::fmt;

use crate::protocol::OwnedCleanup;
use crate::report::AdmissionError;

/// A service resource registration was rejected before ownership transfer.
pub struct ServiceResourceError {
    pub reason: AdmissionError,
    owner: Box<dyn crate::protocol::OwnedCleanup>,
}

impl fmt::Debug for ServiceResourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServiceResourceError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl ServiceResourceError {
    /// Recover the exact unaccepted cleanup owner.
    pub fn into_owner(self) -> Box<dyn crate::protocol::OwnedCleanup> {
        self.owner
    }
}


// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The cleanup payload and error returned when registration fails.

use crate::protocol::OwnedCleanup;
use crate::report::AdmissionError;

/// Preserves cleanup ownership when the cleanup port cannot register it.
pub(crate) struct CleanupRejected {
    /// Describes why cleanup registration was rejected.
    pub error: AdmissionError,
    /// The cleanup value whose registration did not succeed.
    pub cleanup: Box<dyn OwnedCleanup>,
}

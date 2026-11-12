// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The capability that commits a registered child operation.

use crate::report::AdmissionError;

/// Authorizes an already-registered child operation to start when consumed.
pub(crate) trait ChildCommit: Send {
    /// Commits the reservation so the registered operation may begin.
    ///
    /// # Returns
    ///
    /// Returns `()` when the registration is committed.
    ///
    /// # Errors
    ///
    /// Returns the admission failure if the reservation cannot be committed.
    fn commit(self: Box<Self>) -> Result<(), AdmissionError>;
}

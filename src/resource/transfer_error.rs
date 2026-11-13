// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

/// Transfer did not move any registration or cleanup responsibility.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::TransferError;
///
/// let error = TransferError::SameRegistry;
/// assert_eq!(error.to_string(), "resource transfer rejected: SameRegistry");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransferError {
    /// The source and destination are the same registry.
    SameRegistry,
    /// The handle is no longer registered in the stated source registry.
    NotRegistered,
    /// The destination cannot reserve the full dependency group.
    CapacityExceeded,
    /// A registry state was poisoned by a prior panic.
    RegistryPoisoned,
}

impl fmt::Display for TransferError {
    /// Formats the transfer failure with its diagnostic variant name.
    ///
    /// # Parameters
    ///
    /// - `f`: Formatter that receives the diagnostic text.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after writing the diagnostic text.
    ///
    /// # Errors
    ///
    /// Returns the formatter's error if the diagnostic text cannot be written.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resource transfer rejected: {self:?}")
    }
}

impl std::error::Error for TransferError {}

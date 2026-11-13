// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

/// Describes why a resource registry operation could not be confirmed.
///
/// Registry operations return this error when a resource is missing, cleanup
/// state does not match the requested transition, a receipt is invalid, or
/// registry synchronization has been poisoned.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::RegistryError;
///
/// let error = RegistryError::InvalidReceipt;
/// assert!(matches!(error, RegistryError::InvalidReceipt));
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryError {
    /// The resource generation is not registered in this registry.
    NotRegistered,
    /// Cleanup has not been handed out for this resource.
    NotCleaning,
    /// The receipt does not identify this resource generation.
    InvalidReceipt,
    /// No cleanup owner is currently available to restore.
    OwnerUnavailable,
    /// Registry state was poisoned by a prior panic.
    RegistryPoisoned,
}

impl fmt::Display for RegistryError {
    /// Formats the registry failure for diagnostic output.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter receiving the error description.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when formatting succeeds, or the formatter's error
    /// when the output sink rejects the description.
    ///
    /// # Errors
    ///
    /// Returns `fmt::Error` if the formatter cannot accept the description.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resource registry operation failed: {self:?}")
    }
}

impl std::error::Error for RegistryError {}

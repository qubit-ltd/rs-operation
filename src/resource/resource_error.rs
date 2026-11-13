// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

/// Describes why a resource operation could not be completed.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::ResourceError;
///
/// let error = ResourceError::NotRegistered;
/// assert_eq!(error.to_string(), "resource operation failed: NotRegistered");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceError {
    /// New use leases are no longer accepted.
    ClosedForUse,
    /// Cleanup has started or release was confirmed.
    AlreadyCleaning,
    /// The handle no longer names an active registration.
    NotRegistered,
    /// The registry that owned the handle has been dropped.
    RegistryDropped,
    /// Registry state was poisoned by a prior panic.
    RegistryPoisoned,
    /// The number of active leases cannot be represented.
    LeaseCountOverflow,
}

impl fmt::Display for ResourceError {
    /// Formats the error with a stable prefix and its debug variant name.
    ///
    /// # Parameters
    ///
    /// * `f` - The formatter that receives the error description.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when formatting succeeds, or the formatter's error
    /// when writing the description fails.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resource operation failed: {self:?}")
    }
}

impl std::error::Error for ResourceError {}

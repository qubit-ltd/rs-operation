// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Reason a recovery record could not be transferred to a store.
///
/// [`RecoveryHandle::transfer_to`](crate::RecoveryHandle::transfer_to) returns
/// this error when the receiving store rejects ownership.
///
/// # Examples
///
/// ```
/// use qubit_operation::RecoveryTransferError;
///
/// fn needs_capacity(error: RecoveryTransferError) -> bool {
///     error == RecoveryTransferError::CapacityExceeded
/// }
///
/// assert!(needs_capacity(RecoveryTransferError::CapacityExceeded));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryTransferError {
    /// The receiving store is closed.
    Closed,
    /// The receiving store has reached a configured capacity.
    CapacityExceeded,
    /// A store has already accepted this recovery group.
    AlreadyTransferred,
    /// The group is empty, contains duplicate resources, or spans managers.
    InvalidGroup,
}

impl fmt::Display for RecoveryTransferError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// - `self`: Transfer failure to format.
    /// - `f`: Formatter receiving the diagnostic text.
    ///
    /// # Returns
    ///
    /// The formatting result from writing the diagnostic.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for RecoveryTransferError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::RecoveryTransferError;

    #[test]
    fn test_display_is_debug_and_error_has_no_source() {
        let error = RecoveryTransferError::Closed;
        assert_eq!(error.to_string(), format!("{error:?}"));
        assert!(Error::source(&error).is_none());
    }
}

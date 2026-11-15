// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Reason a recovery cleanup retry could not be started.
///
/// # Examples
///
/// ```
/// use qubit_operation::RetryError;
///
/// fn describe(error: RetryError) -> &'static str {
///     match error {
///         RetryError::AlreadyRunning => "cleanup is already running",
///         RetryError::AlreadyResolved => "cleanup is already resolved",
///         RetryError::NoCapacity => "no retry capacity remains",
///         RetryError::ExecutorUnavailable => "retry executor is unavailable",
///     }
/// }
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RetryError {
    /// Another cleanup attempt is already running for this record.
    AlreadyRunning,
    /// Cleanup was already resolved and needs no retry.
    AlreadyResolved,
    /// The recovery store has no remaining retry capacity.
    NoCapacity,
    /// The recovery executor cannot accept the retry task.
    ExecutorUnavailable,
}

impl fmt::Display for RetryError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// - `self`: Retry failure reason being formatted.
    /// - `f`: Formatter receiving the display text.
    ///
    /// # Returns
    ///
    /// A formatting result indicating whether writing succeeded.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for RetryError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::RetryError;

    #[test]
    fn test_display_is_debug_and_error_has_no_source() {
        let error = RetryError::AlreadyRunning;
        assert_eq!(error.to_string(), format!("{error:?}"));
        assert!(Error::source(&error).is_none());
    }
}

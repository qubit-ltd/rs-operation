// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Error from consuming or waiting for an operation outcome.
///
/// # Examples
///
/// Distinguish an unavailable result from a wait that would block its executor:
///
/// ```
/// use qubit_operation::OutcomeError;
///
/// fn outcome_error_kind(error: OutcomeError) -> &'static str {
///     match error {
///         OutcomeError::AlreadyTaken => "already taken",
///         OutcomeError::Discarded => "discarded",
///         OutcomeError::WouldDeadlock => "would deadlock",
///     }
/// }
///
/// assert_eq!(outcome_error_kind(OutcomeError::WouldDeadlock), "would deadlock");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeError {
    /// Another consumer already took the result.
    AlreadyTaken,
    /// The result was discarded before this request.
    Discarded,
    /// Waiting in the current execution context would block its executor.
    WouldDeadlock,
}

impl fmt::Display for OutcomeError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the diagnostic text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for OutcomeError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::OutcomeError;

    #[test]
    fn test_display_is_debug_and_error_has_no_source() {
        let error = OutcomeError::AlreadyTaken;
        assert_eq!(error.to_string(), format!("{error:?}"));
        assert!(Error::source(&error).is_none());
    }
}

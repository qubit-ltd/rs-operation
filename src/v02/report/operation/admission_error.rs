// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Reason a manager rejected an operation submission.
///
/// # Examples
///
/// Separate capacity pressure from lifecycle and execution-context failures:
///
/// ```
/// use qubit_operation::AdmissionError;
///
/// fn rejection_category(error: AdmissionError) -> &'static str {
///     match error {
///         AdmissionError::Busy | AdmissionError::CapacityExceeded => "capacity",
///         AdmissionError::Closed | AdmissionError::ScopeEnded | AdmissionError::Stopped => {
///             "lifecycle"
///         }
///         AdmissionError::DeadlineExpired => "deadline",
///         AdmissionError::ExecutorUnavailable => "executor",
///         AdmissionError::WouldDeadlock => "execution context",
///     }
/// }
///
/// assert_eq!(rejection_category(AdmissionError::CapacityExceeded), "capacity");
/// assert_eq!(rejection_category(AdmissionError::WouldDeadlock), "execution context");
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmissionError {
    /// Immediate admission was requested while capacity was occupied.
    Busy,
    /// The manager no longer accepts submissions.
    Closed,
    /// The operation's parent scope has ended.
    ScopeEnded,
    /// The operation was stopped before admission completed.
    Stopped,
    /// The submission deadline expired before admission.
    DeadlineExpired,
    /// A configured queue or waiter limit was reached.
    CapacityExceeded,
    /// The selected executor cannot accept the operation.
    ExecutorUnavailable,
    /// Blocking for admission would deadlock the current executor.
    WouldDeadlock,
}

impl fmt::Display for AdmissionError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the diagnostic text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for AdmissionError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::AdmissionError;

    #[test]
    fn test_display_is_debug_and_error_has_no_source() {
        let error = AdmissionError::Busy;
        assert_eq!(error.to_string(), format!("{error:?}"));
        assert!(Error::source(&error).is_none());
    }
}

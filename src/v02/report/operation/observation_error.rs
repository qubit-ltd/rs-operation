// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Error from observing operation progress or time.
///
/// # Examples
///
/// Identify why a progress or time observation could not complete:
///
/// ```
/// use qubit_operation::ObservationError;
///
/// fn observation_error_kind(error: ObservationError) -> &'static str {
///     match error {
///         ObservationError::WouldDeadlock => "would deadlock",
///         ObservationError::ClockUnavailable => "clock unavailable",
///         ObservationError::ClockDomainMismatch => "clock domain mismatch",
///     }
/// }
///
/// assert_eq!(
///     observation_error_kind(ObservationError::ClockDomainMismatch),
///     "clock domain mismatch",
/// );
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationError {
    /// The requested wait would block the current executor.
    WouldDeadlock,
    /// No clock was available for the requested observation.
    ClockUnavailable,
    /// The supplied time and configured clock use different domains.
    ClockDomainMismatch,
}

impl fmt::Display for ObservationError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the diagnostic text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for ObservationError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::ObservationError;

    #[test]
    fn test_display_is_debug_and_error_has_no_source() {
        let error = ObservationError::WouldDeadlock;
        assert_eq!(error.to_string(), format!("{error:?}"));
        assert!(Error::source(&error).is_none());
    }
}

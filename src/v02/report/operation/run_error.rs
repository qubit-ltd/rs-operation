// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

use crate::v02::report::ObservationError;
use crate::v02::report::OutcomeError;
use crate::v02::report::SubmitError;

/// Error returned by a convenience runner during submission or observation.
///
/// A submission error retains its uncalled closure so callers can decide
/// whether to retry. Other variants report failures after admission.
///
/// # Type Parameters
///
/// `F` is the submission body retained when admission fails.
///
/// # Examples
///
/// ```
/// use qubit_operation::{AdmissionError, RunError, SubmitError};
///
/// fn inspect(error: RunError<impl FnOnce()>) {
///     if let RunError::Submit(rejected) = error {
///         let (reason, _body) = rejected.into_parts();
///         assert_eq!(reason, AdmissionError::Busy);
///     }
/// }
/// let rejected = SubmitError::new(AdmissionError::Busy, || ());
/// inspect(RunError::Submit(rejected));
/// ```
#[must_use = "the operation runner may retain an uncalled submission body"]
pub enum RunError<F> {
    /// Submission failed and retains the uncalled body.
    Submit(
        /// Rejected submission and its uncalled body.
        SubmitError<F>,
    ),
    /// The result could not be consumed as requested.
    Outcome(
        /// Reason the operation outcome could not be consumed.
        OutcomeError,
    ),
    /// Progress observation failed.
    Observation(
        /// Reason progress observation failed.
        ObservationError,
    ),
}

impl<F> fmt::Debug for RunError<F> {
    /// Formats this value for diagnostic inspection.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the diagnostic representation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Submit(error) => f.debug_tuple("Submit").field(error).finish(),
            Self::Outcome(error) => f.debug_tuple("Outcome").field(error).finish(),
            Self::Observation(error) => f.debug_tuple("Observation").field(error).finish(),
        }
    }
}

impl<F> fmt::Display for RunError<F> {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the diagnostic text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Submit(error) => fmt::Display::fmt(error, f),
            Self::Outcome(error) => fmt::Display::fmt(error, f),
            Self::Observation(error) => fmt::Display::fmt(error, f),
        }
    }
}

impl<F> Error for RunError<F> {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::RunError;
    use crate::v02::report::AdmissionError;
    use crate::v02::report::ObservationError;
    use crate::v02::report::OutcomeError;
    use crate::v02::report::SubmitError;

    #[test]
    fn test_run_error_formats_all_failure_categories() {
        let submit = RunError::Submit(SubmitError::new(AdmissionError::Busy, || ()));
        assert_eq!(submit.to_string(), "submission rejected: Busy");
        assert!(format!("{submit:?}").starts_with("Submit(SubmitError"));
        let outcome: RunError<fn()> = RunError::Outcome(OutcomeError::Discarded);
        assert_eq!(outcome.to_string(), "Discarded");
        assert_eq!(format!("{outcome:?}"), "Outcome(Discarded)");
        let observation: RunError<fn()> = RunError::Observation(ObservationError::ClockUnavailable);
        assert_eq!(observation.to_string(), "ClockUnavailable");
        assert_eq!(format!("{observation:?}"), "Observation(ClockUnavailable)");
        assert!(Error::source(&observation).is_none());
    }
}

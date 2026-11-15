// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

use crate::v02::report::AdmissionError;

/// Rejected submission together with the body that was not called.
///
/// The body can be recovered with [`Self::into_parts`] and retried by the
/// caller after addressing [`Self::reason`].
///
/// # Type Parameters
///
/// `F` is the original callable body retained by the rejection.
///
/// # Examples
///
/// ```
/// use qubit_operation::{AdmissionError, SubmitError};
///
/// let rejected = SubmitError::new(AdmissionError::Busy, || 7);
/// let (reason, body) = rejected.into_parts();
/// assert_eq!(reason, AdmissionError::Busy);
/// assert_eq!(body(), 7);
/// ```
#[must_use = "a rejected submission retains its uncalled body"]
pub struct SubmitError<F> {
    /// Reason the manager rejected the submission.
    reason: AdmissionError,
    /// Original closure, which has not been called.
    body: F,
}

impl<F> SubmitError<F> {
    /// Builds a rejection value while retaining the uncalled body.
    ///
    /// # Parameters
    ///
    /// * `reason` explains why the manager rejected the submission.
    /// * `body` is the original callable that remains uncalled.
    pub fn new(reason: AdmissionError, body: F) -> Self {
        Self { reason, body }
    }
    /// Returns the reason admission failed.
    ///
    /// # Parameters
    ///
    /// * `self` is the rejected submission being inspected.
    pub fn reason(&self) -> AdmissionError {
        self.reason
    }
    /// Recovers both the rejection reason and uncalled body.
    ///
    /// # Parameters
    ///
    /// * `self` is the rejected submission whose parts are returned.
    pub fn into_parts(self) -> (AdmissionError, F) {
        (self.reason, self.body)
    }
}

impl<F> fmt::Debug for SubmitError<F> {
    /// Formats this value for diagnostic inspection.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the diagnostic representation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubmitError")
            .field("reason", &self.reason)
            .field("body", &"<retained>")
            .finish()
    }
}

impl<F> fmt::Display for SubmitError<F> {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the diagnostic text.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "submission rejected: {}", self.reason)
    }
}

impl<F> Error for SubmitError<F> {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::SubmitError;
    use crate::v02::report::AdmissionError;

    #[test]
    fn test_display_debug_and_error_source_preserve_rejection() {
        let error = SubmitError::new(AdmissionError::Busy, || 19);
        assert_eq!(error.to_string(), "submission rejected: Busy");
        assert_eq!(
            format!("{error:?}"),
            r#"SubmitError { reason: Busy, body: "<retained>" }"#
        );
        assert!(Error::source(&error).is_none());
        let (reason, body) = error.into_parts();
        assert_eq!(reason, AdmissionError::Busy);
        assert_eq!(body(), 19);
    }
}

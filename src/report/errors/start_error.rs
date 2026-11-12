// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::fmt;
use std::error::Error;

use super::AdmissionError;

/// Rejected start request retaining the body that was never started.
/// The original body is recoverable because it has not been invoked.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::AdmissionError;
/// use qubit_operation::report::StartError;
///
/// let rejected = StartError::new(AdmissionError::Busy, String::from("request"));
/// let (reason, body) = rejected.into_parts();
/// assert_eq!(reason, AdmissionError::Busy);
/// assert_eq!(body, "request");
/// ```
#[must_use = "handle the rejection or recover the unstarted request body"]
pub struct StartError<F> {
    reason: AdmissionError,
    body: F,
}

impl<F> StartError<F> {
    /// Create a rejection while retaining the unstarted body.
    pub const fn new(reason: AdmissionError, body: F) -> Self {
        Self { reason, body }
    }

    /// Return the admission cause without consuming the retained body.
    pub const fn reason(&self) -> AdmissionError {
        self.reason
    }

    /// Recover the exact body supplied to the rejected start request.
    pub fn into_body(self) -> F {
        self.body
    }

    /// Recover both the admission cause and the exact unstarted body.
    #[must_use = "handle the admission cause and recover the unstarted body"]
    pub fn into_parts(self) -> (AdmissionError, F) {
        (self.reason, self.body)
    }
}

impl<F> fmt::Debug for StartError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StartError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}

impl<F> fmt::Display for StartError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(f)
    }
}

impl<F> Error for StartError<F> {}

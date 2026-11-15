// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::ExecutorError;

#[cfg(test)]
/// A rejected submission that returns both the admission error and original
/// job.
///
/// # Type Parameters
///
/// * `F` is the job that the caller may retry or clean up.
pub(crate) struct SubmitFailure<F> {
    /// Reason the executor rejected this job.
    pub(super) error: ExecutorError,
    /// Original job value retained for the caller.
    pub(super) job: F,
}

impl<F> std::fmt::Debug for SubmitFailure<F> {
    /// Formats only the rejection reason without requiring the job to implement
    /// Debug.
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SubmitFailure")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl<F> SubmitFailure<F> {
    /// Returns why the job was rejected while preserving it for retry.
    pub(crate) fn error(&self) -> ExecutorError {
        self.error
    }

    /// Returns ownership of the rejected job to the caller.
    pub(crate) fn into_job(self) -> F {
        self.job
    }
}

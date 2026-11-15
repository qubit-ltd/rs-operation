// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::resource::recovery::RecoveryAttempt;

/// Asynchronous observer for one admitted recovery retry.
///
/// This wrapper shares the retry run and completion signal with its synchronous
/// [`RecoveryAttempt`] observers. Dropping the wrapper does not cancel cleanup.
///
/// # Examples
///
/// A recovery handle is delivered in a resource-scope report. Convert its
/// admitted retry to an asynchronous observer to await it without blocking:
///
/// ```
/// use qubit_operation::ResourceScopeReport;
///
/// async fn observe_recovery(report: &ResourceScopeReport) {
///     let Some(handle) = report.recoveries.first() else {
///         return;
///     };
///     let Ok(attempt) = handle.try_retry() else {
///         return;
///     };
///     let async_attempt = attempt.as_async();
///     let result = async_attempt.wait().await;
///     let _phase = result.recovery.phase;
/// }
/// ```
pub struct AsyncRecoveryAttempt {
    /// Synchronous observer that owns the shared retry observation state.
    pub(in crate::v02::resource::recovery) attempt: RecoveryAttempt,
}

impl AsyncRecoveryAttempt {
    /// Waits asynchronously for the admitted retry to publish its report.
    ///
    /// Polling this future starts the retry unless another observer already
    /// started the shared run.
    ///
    /// # Parameters
    ///
    /// * `self` — observer for the admitted recovery retry.
    ///
    /// # Returns
    ///
    /// The report containing the recovery snapshot and attempt failures.
    pub async fn wait(&self) -> crate::v02::report::RecoveryAttemptReport {
        self.attempt.wait_async().await
    }

    /// Converts this asynchronous observer into its synchronous counterpart.
    ///
    /// The returned observer keeps the same in-flight run and completion
    /// signal; it does not start a second retry.
    ///
    /// # Parameters
    ///
    /// * `self` — asynchronous observer to convert.
    ///
    /// # Returns
    ///
    /// A synchronous observer for the same admitted retry.
    pub fn as_sync(self) -> RecoveryAttempt {
        self.attempt
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use crate::v02::report::CleanupFailure;
use crate::v02::resource::recovery::AsyncRecoveryAttempt;
use crate::v02::resource::recovery::AttemptObservation;
use crate::v02::resource::recovery::RecoveryCore;
use crate::v02::resource::recovery::RecoveryRun;
use crate::v02::resource::recovery::block_on;
use crate::v02::resource::recovery::lock;
use crate::v02::resource::recovery::publish_attempt;
use crate::v02::resource::recovery::wait_attempt_async;

/// One admitted retry and its shared observation state.
///
/// A synchronous or asynchronous observer can wait on the same attempt. If
/// every observer is dropped before completion, the retained owner remains in
/// recovery and its phase becomes `Unconfirmed`; it is never marked released.
/// Attempts are created by [`crate::RecoveryHandle::try_retry`] or
/// [`crate::RecoveryStore::try_retry`].
///
/// # Examples
///
/// ```
/// use qubit_operation::RetryError;
/// use qubit_operation::RecoveryAttempt;
/// use qubit_operation::RecoveryHandle;
///
/// fn begin_retry(handle: &RecoveryHandle) -> Result<RecoveryAttempt, RetryError> {
///     handle.try_retry()
/// }
/// ```
#[must_use = "wait for or retain the admitted recovery attempt"]
pub struct RecoveryAttempt {
    /// Recovery group whose retained owner is being retried.
    pub(in crate::v02::resource::recovery) core: Arc<RecoveryCore>,
    /// In-flight run shared by synchronous and asynchronous observers.
    pub(in crate::v02::resource::recovery) run: Arc<Mutex<Option<RecoveryRun>>>,
    /// Completion signal and report shared by all attempt observers.
    pub(in crate::v02::resource::recovery) observation: Arc<AttemptObservation>,
}

impl RecoveryAttempt {
    /// Blocks the current thread until this retry publishes its report.
    ///
    /// The retry starts when this method is first called unless another
    /// observer already took the shared run.
    ///
    /// # Parameters
    ///
    /// - `self`: Observer for the admitted retry.
    ///
    /// # Returns
    ///
    /// The report published after the attempt completes.
    ///
    /// # Errors
    ///
    /// The result wrapper is retained for observation API consistency; waiting
    /// currently returns the published report in `Ok`.
    pub fn wait(&self) -> Result<crate::v02::report::RecoveryAttemptReport, crate::v02::report::ObservationError> {
        Ok(block_on(self.wait_async()))
    }

    /// Creates an asynchronous observer for this same retry.
    ///
    /// The returned observer shares the in-flight run and completion signal;
    /// it does not admit a second cleanup attempt.
    ///
    /// # Parameters
    ///
    /// - `self`: Observer whose shared attempt is wrapped.
    ///
    /// # Returns
    ///
    /// An asynchronous observer for the same admitted retry.
    #[must_use = "wait for or retain the asynchronous recovery observer"]
    pub fn as_async(&self) -> AsyncRecoveryAttempt {
        AsyncRecoveryAttempt {
            attempt: self.clone_observer(),
        }
    }

    /// Starts or observes this retry without blocking the current thread.
    ///
    /// The retry action starts when this future is first polled. If an executor
    /// permit was reserved, the action is spawned there; otherwise the action
    /// is awaited directly. All observers receive the same completion report.
    ///
    /// # Parameters
    ///
    /// - `self`: Observer for the admitted retry.
    ///
    /// # Returns
    ///
    /// The report containing the recovery snapshot and failures from the
    /// completed attempt.
    pub async fn wait_async(&self) -> crate::v02::report::RecoveryAttemptReport {
        let run = { lock(&self.run).take() };
        if let Some(mut run) = run {
            if let Some(permit) = run.permit.take() {
                let observation = Arc::clone(&self.observation);
                permit.spawn(async move {
                    let payload = run.payload.as_ref().expect("attempt owns payload").clone();
                    let result = run.action.retry(payload).await;
                    run.complete(result);
                    publish_attempt(&observation, run.core.snapshot());
                });
            } else {
                let payload = run.payload.as_ref().expect("attempt owns payload").clone();
                let result = run.action.retry(payload).await;
                run.complete(result);
                publish_attempt(&self.observation, self.core.snapshot());
            }
        }
        wait_attempt_async(&self.observation).await
    }

    /// Completes a still-owned run with a caller-supplied cleanup result.
    ///
    /// `Ok(())` resolves the recovery group; an error records the failure and
    /// returns the group to a retryable phase. If a waiter already took the
    /// run, this call leaves report publication to that waiter.
    ///
    /// # Parameters
    ///
    /// - `self`: Observer whose run is being completed.
    /// - `result`: Confirmation or failure produced by the cleanup action.
    pub fn finish(&self, result: Result<(), CleanupFailure>) {
        let run = lock(&self.run).take();
        if let Some(mut run) = run {
            run.complete(result);
            publish_attempt(&self.observation, self.core.snapshot());
        }
    }

    /// Clones only the shared observation handles, not the recovery run.
    ///
    /// # Parameters
    ///
    /// - `self`: Observer whose shared state is cloned.
    ///
    /// # Returns
    ///
    /// Another observer of the same attempt.
    fn clone_observer(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            run: Arc::clone(&self.run),
            observation: Arc::clone(&self.observation),
        }
    }
}

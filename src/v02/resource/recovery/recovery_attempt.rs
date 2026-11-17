// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;

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
/// A synchronous or asynchronous observer can wait on the same attempt. In
/// direct execution, the first observer polled drives the retry; dropping that
/// pending wait preserves its cleanup future, records `Unconfirmed`, and wakes
/// all other observers. Dropping a non-driving observer only cancels that wait.
/// When execution was handed to a store worker, dropping an observer does not
/// stop the worker. Panics from retry action construction or polling are also
/// reported as `Unconfirmed`, with the resource responsibility retained.
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

/// Drops a completed retry future inside an unwind boundary.
///
/// A destructor panic means the cleanup result cannot be trusted. The future
/// has already been removed from its owner and must never be polled or dropped
/// again.
///
/// # Parameters
///
/// * `future` — completed retry future to destroy behind the unwind boundary.
///
/// # Returns
///
/// `Ok(())` when destruction succeeds, or the panic payload when it unwinds.
pub(super) fn drop_completed_future(
    future: crate::v02::resource::recovery::RecoveryFuture,
) -> Result<(), Box<dyn std::any::Any + Send + 'static>> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(future))) {
        Ok(()) => Ok(()),
        Err(panic) => Err(panic),
    }
}

/// Disposes of a value abandoned after the completed future's Drop panicked.
///
/// # Parameters
///
/// * `result` — completed retry result whose destructor must not escape.
fn dispose_abandoned_result(result: Result<(), CleanupFailure>) {
    if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(result))) {
        crate::v02::manager::core::dispose_panic_payload(panic);
    }
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
    /// permit was reserved, the action is spawned there and dropping this wait
    /// only cancels observation. Otherwise, the first polled observer drives
    /// the action directly; dropping its pending wait retains the unfinished
    /// action future, records `Unconfirmed`, and wakes the other observers.
    /// All observers receive the same completion report.
    ///
    /// # Parameters
    ///
    /// - `self`: Observer for the admitted retry.
    ///
    /// # Returns
    ///
    /// The report containing the recovery snapshot and failures from the
    /// completed attempt. An unwind panic from retry construction or polling
    /// produces an `Unconfirmed` report rather than escaping this wait.
    pub async fn wait_async(&self) -> crate::v02::report::RecoveryAttemptReport {
        let run = { lock(&self.run).take() };
        if let Some(mut run) = run {
            if let Some(permit) = run.permit.take() {
                let observation = Arc::clone(&self.observation);
                permit.spawn(async move {
                    let payload = run.payload.as_ref().expect("attempt owns payload").clone();
                    match retry_catching_panic(Arc::clone(&run.action), payload).await {
                        Ok(result) => run.complete(result),
                        Err((panic, future)) => run.complete_unconfirmed(panic, future),
                    }
                    publish_attempt(&observation, run.core.snapshot());
                });
            } else {
                super::direct_recovery_driver::drive_direct(run, Arc::clone(&self.observation)).await;
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

/// Runs action construction and polling behind unwind boundaries.
///
/// # Parameters
///
/// * `action` — recovery strategy whose construction and polling are guarded.
/// * `payload` — retained owner payload supplied to the recovery strategy.
///
/// # Returns
///
/// The action result, or the panic payload with any future that must remain
/// retained because its cleanup outcome is unknown.
async fn retry_catching_panic(
    action: Arc<dyn crate::v02::resource::recovery::RecoveryAction>,
    payload: crate::v02::resource::recovery::RecoveryPayload,
) -> Result<
    Result<(), CleanupFailure>,
    (
        Box<dyn std::any::Any + Send>,
        Option<crate::v02::resource::recovery::RecoveryFuture>,
    ),
> {
    let future = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| action.retry(payload))) {
        Ok(future) => future,
        Err(panic) => return Err((panic, None)),
    };
    let mut future = Some(future);
    std::future::poll_fn(|cx| {
        let future_ref = future.as_mut().expect("future remains present until completion");
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future_ref.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => {
                let completed = future.take().expect("completed future remains present");
                match drop_completed_future(completed) {
                    Ok(()) => Poll::Ready(Ok(result)),
                    Err(panic) => {
                        dispose_abandoned_result(result);
                        Poll::Ready(Err((panic, None)))
                    }
                }
            }
            Err(panic) => Poll::Ready(Err((panic, future.take()))),
        }
    })
    .await
}

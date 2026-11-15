// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Arc;
use std::sync::Mutex;

use super::AdmissionError;
use super::AsyncOperationHandle;
use super::dispose_panic_payload;
use super::ChildRelease;
use super::DiagnosticSummary;
use super::ExecutionFrameGuard;
use super::ExecutionKind;
use super::ExecutionPhase;
use super::ManagedOperationControl;
use super::ManagerCore;
use super::OperationCore;
#[cfg(test)]
use super::OperationId;
use super::OperationJob;
use super::OperationManager;
use super::OperationOptions;
use super::OperationOutcome;
use super::OperationScope;
use super::OutcomeSlot;
use super::PanicSummary;
use super::RunnerParts;
use super::StopControl;
use super::SubmissionTarget;
use super::SubmitError;
use super::child_outcome_summary;
use super::lock;
use super::panic_message;
use crate::v02::scope::ExecutionContextFuture;
use crate::v02::scope::ExecutionContextGuard;

/// An async view over the shared coordinator and one fixed target.
///
/// # Examples
///
/// ```
/// use qubit_operation::AsyncOperationManager;
/// use qubit_operation::ManagerOptions;
///
/// let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
/// manager.close_admission();
/// ```
#[must_use = "a manager view is required to submit and coordinate operations"]
#[derive(Clone)]
pub struct AsyncOperationManager {
    /// Shared coordinator that keeps manager state alive across manager views.
    core: Arc<ManagerCore>,
    /// Submission target fixed for this view, such as a root or child group.
    target: SubmissionTarget,
}
impl AsyncOperationManager {
    /// Constructs an async view with real bounded executors and cleanup.
    ///
    /// # Parameters
    ///
    /// - `options`: Executor, admission, and cleanup configuration for the
    ///   manager.
    ///
    /// # Returns
    ///
    /// The configured async manager, or an error if its production resources
    /// cannot be created.
    ///
    /// # Errors
    ///
    /// Returns [`ManagerCreateError`](crate::v02::report::ManagerCreateError)
    /// when a configured executor, adapter, or cleanup resource cannot be
    /// initialized.
    pub fn new(options: crate::v02::report::ManagerOptions) -> Result<Self, crate::v02::report::ManagerCreateError> {
        crate::v02::manager::production::new_async(options)
    }

    /// Builds an async manager view over an existing coordinator and target.
    ///
    /// The shared `Arc` preserves the coordinator lifetime while this view
    /// keeps submissions bound to the supplied target.
    ///
    /// # Parameters
    ///
    /// - `core`: Shared coordinator that owns manager state.
    /// - `target`: Submission target fixed for the returned manager view.
    ///
    /// # Returns
    ///
    /// An async manager view bound to the supplied coordinator and target.
    #[inline]
    pub(super) fn from_parts(core: Arc<ManagerCore>, target: SubmissionTarget) -> Self {
        Self { core, target }
    }

    /// Closes admission while allowing already accepted operations to drain.
    ///
    /// This is equivalent to requesting
    /// [`ShutdownMode::Drain`](crate::v02::report::ShutdownMode::Drain).
    ///
    /// This method has no effect on operations that were already accepted.
    #[inline]
    pub fn close_admission(&self) {
        self.as_sync().close_admission();
    }

    /// Requests shutdown and returns an async handle for observing completion.
    ///
    /// `Drain` stops new submissions and waits for accepted work; `Cancel` also
    /// requests cancellation of work that has not completed.
    ///
    /// # Parameters
    ///
    /// - `mode`: Whether shutdown drains accepted work or requests
    ///   cancellation.
    ///
    /// # Returns
    ///
    /// A handle that observes completion of the requested shutdown.
    #[inline]
    pub fn request_shutdown(
        &self,
        mode: crate::v02::report::ShutdownMode,
    ) -> crate::v02::manager::shutdown::AsyncShutdownHandle {
        self.as_sync().request_shutdown(mode).as_async()
    }

    /// Returns the corresponding sync view without changing its target.
    ///
    /// # Returns
    ///
    /// A synchronous manager view sharing this manager's coordinator and
    /// target.
    #[inline]
    pub fn as_sync(&self) -> OperationManager {
        OperationManager::from_parts(Arc::clone(&self.core), self.target.clone())
    }

    /// Submits an async operation, awaiting retryable capacity pressure in its
    /// group, manager, or selected executor.
    ///
    /// The body is retained and retried only when admission reports `Busy`;
    /// other admission failures return it to the caller in `SubmitError`.
    /// Dropping this future unregisters its pending capacity waiter. Waiting is
    /// bounded by the manager's admission-waiter limit and does not guarantee
    /// FIFO ordering. Root submissions are rejected after manager Drain begins,
    /// while descendants of accepted scopes may continue.
    /// Panics while constructing, polling, or finally dropping the returned
    /// future are reported as [`OperationOutcome::Panicked`] and still
    /// complete operation settlement when the panic is unwind-catchable.
    /// Panics configured to abort the process cannot be recovered.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Value produced by the operation body.
    /// - `E`: Error produced by the operation body.
    /// - `F`: One-shot closure that creates the async body from its scope.
    /// - `Fut`: Future returned by the body closure.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission, resource, and execution settings for the
    ///   operation.
    /// - `body`: Closure that receives the operation scope and returns its
    ///   future.
    ///
    /// # Returns
    ///
    /// A handle for observing the accepted operation's outcome and settlement.
    ///
    /// # Errors
    ///
    /// Returns the original body with the admission or operation setup error.
    pub async fn submit<T, E, F, Fut>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<AsyncOperationHandle<T, E>, SubmitError<F>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let mut body = Some(body);
        loop {
            match self.try_submit(
                options.clone(),
                body.take()
                    .expect("async submission body remains available until accepted"),
            ) {
                Ok(handle) => return Ok(handle),
                Err(error) => {
                    let (reason, original) = error.into_parts();
                    if reason != AdmissionError::Busy {
                        return Err(SubmitError::new(reason, original));
                    }
                    if let Err(reason) = self
                        .core
                        .wait_for_retry_capacity(&self.target, ExecutionKind::Async, options.clone())
                        .await
                    {
                        return Err(SubmitError::new(reason, original));
                    }
                    body = Some(original);
                }
            }
        }
    }

    /// Attempts to submit an async operation without waiting for capacity.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Value produced by the operation body.
    /// - `E`: Error produced by the operation body.
    /// - `F`: One-shot closure that creates the async body from its scope.
    /// - `Fut`: Future returned by the body closure.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission, resource, and execution settings for the
    ///   operation.
    /// - `body`: Closure that receives the operation scope and returns its
    ///   future.
    ///
    /// # Returns
    ///
    /// A handle for the accepted operation, or the original body with the
    /// admission error if submission is rejected.
    ///
    /// # Errors
    ///
    /// Returns immediately with the original body when admission is rejected.
    pub fn try_submit<T, E, F, Fut>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<AsyncOperationHandle<T, E>, SubmitError<F>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let body_cell = Arc::new(Mutex::new(Some(body)));
        let body_for_job = Arc::clone(&body_cell);
        let parts: Arc<Mutex<Option<RunnerParts<T, E>>>> = Arc::new(Mutex::new(None));
        let parts_for_job = Arc::clone(&parts);
        let runner = move |scope: OperationScope| {
            let body = lock(&body_for_job).take().expect("accepted async body is present");
            let (operation, slot, lease, parent, control) = lock(&parts_for_job)
                .take()
                .expect("accepted async runner capsule is present");
            let scope_core = Arc::clone(&scope.core);
            let operation_id = scope.id();
            async move {
                let frame = ExecutionFrameGuard::new(Arc::clone(&scope_core));
                operation.set_execution(ExecutionPhase::Running);
                let manager_core = scope_core.manager().upgrade().expect("active scope manager exists");
                let group = scope_core.group().cloned();
                let construction = {
                    let _context = ExecutionContextGuard::enter(&manager_core, operation_id, group.as_ref());
                    catch_unwind(AssertUnwindSafe(|| body(scope)))
                };
                let result = match construction {
                    Ok(user_future) => {
                        ExecutionContextFuture::new(
                            user_future,
                            manager_core,
                            operation_id,
                            group,
                        )
                        .await
                    }
                    Err(payload) => Err(payload),
                };
                operation.set_execution(ExecutionPhase::Finished);
                lease.execution_finished();
                control.confirm_input_release(ChildRelease {
                    operation_id,
                    confirmed_at: None,
                });
                let callback_scope = Arc::clone(&scope_core);
                scope_core.on_outcomes_ready(move || {
                    let outcome = match result {
                        Ok(result) => callback_scope.resolve_outcome(result),
                        Err(payload) => {
                            let summary = DiagnosticSummary::new(&panic_message(payload.as_ref()));
                            dispose_panic_payload(payload);
                            OperationOutcome::Panicked(PanicSummary {
                                operation_id,
                                phase: ExecutionPhase::Running,
                                summary,
                            })
                        }
                    };
                    let summary = child_outcome_summary(operation_id, &outcome);
                    control.publish_outcome(summary.clone());
                    if let Some(parent) = parent {
                        let _ = parent.record_child_outcome(summary);
                    }
                    let _ = slot.publish(outcome);
                });
                drop(frame);
                scope_core.mark_body_finished();
            }
        };
        let (runner, id, scope, reservation) =
            match self
                .core
                .reserve_scope(&self.target, options, ExecutionKind::Async, runner)
            {
                Ok(result) => result,
                Err(error) => {
                    let (reason, _runner) = error.into_parts();
                    let body = lock(&body_cell).take().expect("rejected async body is retained");
                    return Err(SubmitError::new(reason, body));
                }
            };
        let operation = OperationCore::new(id);
        scope.bind_operation_core(&operation);
        let slot = Arc::new(OutcomeSlot::new(Arc::clone(&operation)));
        let lease = Arc::clone(&reservation.lifecycle);
        slot.set_delivery_hook({
            let lease = Arc::clone(&lease);
            move |delivery| lease.outcome_delivered(delivery)
        });
        let parent = scope.parent();
        let control = ManagedOperationControl::new(Arc::clone(&operation), Arc::clone(&scope));
        self.core.register_settlement(
            Arc::clone(&scope),
            Arc::clone(&operation),
            Arc::clone(&lease),
            Arc::clone(&control),
        );
        *lock(&parts) = Some((
            Arc::clone(&operation),
            Arc::clone(&slot),
            Arc::clone(&lease),
            parent,
            Arc::clone(&control),
        ));
        let handle = AsyncOperationHandle::new(
            Arc::clone(&operation),
            Arc::clone(&slot),
            Arc::clone(&control) as Arc<dyn StopControl>,
        );
        let future = runner(OperationScope::new(Arc::clone(&scope), Arc::clone(&self.core)));
        let abandon_scope = Arc::clone(&scope);
        let abandon_operation = Arc::clone(&operation);
        let abandon_slot = Arc::clone(&slot);
        let abandon_lease = Arc::clone(&lease);
        let abandon_parent = scope.parent();
        let abandon_control = Arc::clone(&control);
        let execution = reservation.execution;
        let _ = self.core.dispatch_adapter_job(
            Arc::clone(&scope),
            Arc::clone(&control),
            execution,
            OperationJob::Async(Box::pin(future)),
            move || {
                ManagerCore::abandon_reserved_operation(
                    abandon_scope,
                    abandon_operation,
                    abandon_slot,
                    abandon_lease,
                    abandon_parent,
                    abandon_control,
                );
            },
        );
        Ok(handle)
    }

    /// Submits an async operation and waits for its outcome.
    ///
    /// This returns when the outcome is available; resource settlement may
    /// still be in progress.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Value produced by the operation body.
    /// - `E`: Error produced by the operation body.
    /// - `F`: One-shot closure that creates the async body from its scope.
    /// - `Fut`: Future returned by the body closure.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission, resource, and execution settings for the
    ///   operation.
    /// - `body`: Closure that receives the operation scope and returns its
    ///   future.
    ///
    /// # Returns
    ///
    /// The operation's outcome as soon as it becomes available.
    ///
    /// # Errors
    ///
    /// Returns submission or outcome-observation failure, retaining the body
    /// when submission is rejected.
    pub async fn run<T, E, F, Fut>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationOutcome<T, E>, crate::v02::report::RunError<F>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let mut operation = self
            .submit(options, body)
            .await
            .map_err(crate::v02::report::RunError::Submit)?;
        operation
            .wait_outcome()
            .await
            .map_err(crate::v02::report::RunError::Outcome)
    }

    /// Submits an async operation and waits for both outcome and settlement.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Value produced by the operation body.
    /// - `E`: Error produced by the operation body.
    /// - `F`: One-shot closure that creates the async body from its scope.
    /// - `Fut`: Future returned by the body closure.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission, resource, and execution settings for the
    ///   operation.
    /// - `body`: Closure that receives the operation scope and returns its
    ///   future.
    ///
    /// # Returns
    ///
    /// The outcome and settlement reports after both become observable.
    ///
    /// # Errors
    ///
    /// Returns submission, outcome-observation, or settlement-observation
    /// failure, retaining the body when submission is rejected.
    pub async fn run_and_settle<T, E, F, Fut>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<crate::v02::report::OperationCompletion<T, E>, crate::v02::report::RunError<F>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        let mut operation = self
            .submit(options, body)
            .await
            .map_err(crate::v02::report::RunError::Submit)?;
        let outcome = operation
            .wait_outcome()
            .await
            .map_err(crate::v02::report::RunError::Outcome)?;
        let settlement = operation.wait_settlement().await;
        Ok(crate::v02::report::OperationCompletion { outcome, settlement })
    }

    /// Submits an async unit-returning body through the test execution port.
    ///
    /// # Type Parameters
    ///
    /// - `F`: One-shot closure that creates the async body from its scope.
    /// - `Fut`: Future returned by the body closure.
    ///
    /// # Parameters
    ///
    /// - `body`: Closure that receives the operation scope and returns its
    ///   future.
    #[cfg(test)]
    pub(crate) fn try_submit_body<F, Fut>(&self, body: F) -> Result<OperationId, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.core
            .submit_async_scope(&self.target, OperationOptions::default(), body)
    }
}

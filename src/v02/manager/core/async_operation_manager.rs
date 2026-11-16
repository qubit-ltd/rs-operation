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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use super::AdmissionError;
use super::AsyncOperationHandle;
use super::ChildRelease;
use super::DiagnosticSummary;
use super::ExecutionFrameGuard;
use super::ExecutionKind;
use super::ExecutionPhase;
use super::ExternalBindResult;
use super::ExternalExecutionContext;
use super::ExternalOutcomePublisher;
use super::ExternalStopControl;
use super::ManagedOperationControl;
use super::ManagerCore;
use super::OperationCore;
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
use super::dispose_panic_payload;
use super::drive_external;
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
    /// Attempts to submit a prepared external operation.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Successful value produced by the external task.
    /// - `E`: Error value produced by the external task.
    /// - `P`: Prepared external task implementation.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission and resource settings for the operation.
    /// - `prepared`: Task that the manager binds and drives asynchronously.
    ///
    /// # Returns
    ///
    /// A handle for the accepted operation, or the original task if rejected.
    ///
    /// # Errors
    ///
    /// Returns the original task with the admission error when it is rejected.
    pub fn try_submit_external<T, E, P>(
        &self,
        options: OperationOptions,
        prepared: Arc<P>,
    ) -> Result<AsyncOperationHandle<T, E>, SubmitError<Arc<P>>>
    where
        T: Send + 'static,
        E: Send + 'static,
        P: crate::v02::adapter::PreparedExternalOperation<T, E>,
    {
        let bind_result: ExternalBindResult = Arc::new(Mutex::new(None));
        let managed_control: Arc<Mutex<Option<Arc<ManagedOperationControl>>>> =
            Arc::new(Mutex::new(None));
        let execution_context: Arc<Mutex<Option<Arc<ExternalExecutionContext>>>> =
            Arc::new(Mutex::new(None));
        let outcome_publisher: ExternalOutcomePublisher<T, E> = Arc::new(Mutex::new(None));
        let activation_permitted = Arc::new(AtomicBool::new(false));
        let state_for_preflight = Arc::clone(&bind_result);
        let adapter_for_preflight = Arc::clone(&prepared);
        let preflight = move |id| {
            let result = catch_unwind(AssertUnwindSafe(|| adapter_for_preflight.bind(id)));
            match result {
                Ok(Ok(())) => *lock(&state_for_preflight) = Some(Ok(())),
                Ok(Err(crate::v02::adapter::ExternalBindError::AlreadyBound)) => {
                    return Err(AdmissionError::AlreadyBound);
                }
                Ok(Err(crate::v02::adapter::ExternalBindError::Failed(failure))) => {
                    *lock(&state_for_preflight) = Some(Err(failure));
                }
                Err(payload) => {
                    *lock(&state_for_preflight) = Some(Err(crate::v02::report::ExecutionFailure {
                        operation_id: id,
                        summary: DiagnosticSummary::new(&panic_message(payload.as_ref())),
                        source: None,
                    }));
                }
            }
            Ok(())
        };
        let bind_state = Arc::clone(&bind_result);
        let managed_state = Arc::clone(&managed_control);
        let execution_state = Arc::clone(&execution_context);
        let publisher_state = Arc::clone(&outcome_publisher);
        let activation_state = Arc::clone(&activation_permitted);
        let adapter_for_runner = Arc::clone(&prepared);
        let runner = move |scope: OperationScope| {
            let bind_state = Arc::clone(&bind_state);
            let managed_state = Arc::clone(&managed_state);
            let execution_state = Arc::clone(&execution_state);
            let publisher_state = Arc::clone(&publisher_state);
            let adapter = Arc::clone(&adapter_for_runner);
            let activation_state = Arc::clone(&activation_state);
            async move {
                drive_external(
                    scope,
                    adapter,
                    bind_state,
                    managed_state,
                    execution_state,
                    publisher_state,
                    activation_state.load(Ordering::Acquire),
                )
                .await;
            }
        };
        self.try_submit_external_raw(
            options,
            runner,
            preflight,
            Arc::clone(&managed_control),
            Arc::clone(&execution_context),
            Arc::clone(&outcome_publisher),
            Arc::clone(&activation_permitted),
        )
        .map_err(|(reason, _runner)| SubmitError::new(reason, prepared))
    }

    /// Admits and dispatches an external asynchronous operation body.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Successful value produced by the operation.
    /// - `E`: Error value produced by the operation.
    /// - `F`: Body that constructs the asynchronous operation future.
    /// - `Fut`: Future driven for the accepted operation.
    /// - `C`: One-shot admission preflight callback.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission and scope settings for the operation.
    /// - `body`: Operation body retained until dispatch.
    /// - `preflight`: Identity claim run after admission succeeds.
    /// - `external_managed`: Slot for the manager control installed before dispatch.
    /// - `external_execution`: Slot for execution lifecycle state.
    /// - `early_publisher`: Callback that receives the operation outcome.
    /// - `activation_permitted`: Whether lifecycle state allows body activation.
    ///
    /// # Returns
    ///
    /// The accepted operation handle, or the admission error with the body.
    ///
    /// # Errors
    ///
    /// Returns the body with the admission error when the operation is rejected.
    fn try_submit_external_raw<T, E, F, Fut, C>(
        &self,
        options: OperationOptions,
        body: F,
        preflight: C,
        external_managed: Arc<Mutex<Option<Arc<ManagedOperationControl>>>>,
        external_execution: Arc<Mutex<Option<Arc<ExternalExecutionContext>>>>,
        early_publisher: ExternalOutcomePublisher<T, E>,
        activation_permitted: Arc<AtomicBool>,
    ) -> Result<AsyncOperationHandle<T, E>, (AdmissionError, F)>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
        C: FnOnce(OperationId) -> Result<(), AdmissionError>,
    {
        let body_cell = Arc::new(Mutex::new(Some(body)));
        let body_for_job = Arc::clone(&body_cell);
        let parts: Arc<Mutex<Option<RunnerParts<T, E>>>> = Arc::new(Mutex::new(None));
        let parts_for_job = Arc::clone(&parts);
        let early_published = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let early_published_for_runner = Arc::clone(&early_published);
        let execution_for_runner = Arc::clone(&external_execution);
        let runner = move |scope: OperationScope| {
            let body = lock(&body_for_job)
                .take()
                .expect("accepted async external body is present");
            let (operation, slot, lease, parent, control) = lock(&parts_for_job)
                .take()
                .expect("accepted async external runner capsule is present");
            let scope_core = Arc::clone(&scope.core);
            let operation_id = scope.id();
            let frame = ExecutionFrameGuard::new(Arc::clone(&scope_core));
            operation.set_execution(ExecutionPhase::Running);
            *lock(&execution_for_runner) = Some(Arc::new(ExternalExecutionContext::new(
                Arc::clone(&operation),
                Arc::clone(&lease),
                Arc::clone(&scope_core),
                Some(frame),
            )));
            let manager_core = scope_core
                .manager()
                .upgrade()
                .expect("active async scope manager exists");
            let groups = scope_core.groups().to_vec();
            let body_future = {
                let _context = ExecutionContextGuard::enter(&manager_core, operation_id, &groups);
                body(scope)
            };
            async move {
                let result =
                    ExecutionContextFuture::new(body_future, manager_core, operation_id, groups)
                        .await;
                if let Some(context) = lock(&execution_for_runner).as_ref() {
                    context.finish();
                }
                if !early_published_for_runner.load(std::sync::atomic::Ordering::Acquire) {
                    scope_core.on_outcomes_ready(move || {
                        let outcome = match result {
                            Ok(()) => OperationOutcome::ExecutionFailed(
                                crate::v02::report::ExecutionFailure {
                                    operation_id,
                                    summary: DiagnosticSummary::new(
                                        "async external driver ended without an outcome",
                                    ),
                                    source: None,
                                },
                            ),
                            Err(payload) => OperationOutcome::Panicked(PanicSummary {
                                operation_id,
                                phase: ExecutionPhase::Running,
                                summary: DiagnosticSummary::new(&panic_message(payload.as_ref())),
                            }),
                        };
                        let summary = child_outcome_summary(operation_id, &outcome);
                        control.publish_outcome(summary.clone());
                        if let Some(parent) = parent {
                            let _ = parent.record_child_outcome(summary);
                        }
                        let _ = slot.publish(outcome);
                    });
                }
                scope_core.mark_body_finished();
            }
        };
        let (runner, id, scope, reservation) = match self.core.reserve_scope_with_preflight(
            &self.target,
            options,
            ExecutionKind::Async,
            runner,
            preflight,
            true,
        ) {
            Ok(result) => result,
            Err(error) => {
                let (reason, runner) = error.into_parts();
                drop(runner);
                return Err((
                    reason,
                    lock(&body_cell)
                        .take()
                        .expect("rejected async body is retained"),
                ));
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
        *lock(&external_managed) = Some(Arc::clone(&control));
        let scope_for_publisher = Arc::clone(&scope);
        let control_for_publisher = Arc::clone(&control);
        let slot_for_publisher = Arc::clone(&slot);
        let parent_for_publisher = parent.clone();
        let flag_for_callback = Arc::clone(&early_published);
        *lock(&early_publisher) = Some(Box::new(move |outcome| {
            if flag_for_callback.swap(true, std::sync::atomic::Ordering::AcqRel) {
                return;
            }
            let callback_scope = Arc::clone(&scope_for_publisher);
            let callback_scope_for_outcome = Arc::clone(&callback_scope);
            callback_scope.on_outcomes_ready(move || {
                let outcome = callback_scope_for_outcome.resolve_typed_outcome(outcome);
                let summary = child_outcome_summary(callback_scope_for_outcome.id(), &outcome);
                control_for_publisher.publish_outcome(summary.clone());
                if let Some(parent) = parent_for_publisher {
                    let _ = parent.record_child_outcome(summary);
                }
                let _ = slot_for_publisher.publish(outcome);
            });
        }));
        scope.retain_external_driver_settlement();
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
        let stop: Arc<dyn StopControl> = Arc::new(ExternalStopControl {
            managed: Arc::clone(&control),
        });
        let handle = AsyncOperationHandle::new(Arc::clone(&operation), Arc::clone(&slot), stop);
        let start_permitted = scope.commit_and_activate_start().is_ok();
        activation_permitted.store(start_permitted, Ordering::Release);
        let execution = reservation.execution;
        let future = runner(OperationScope::new(
            Arc::clone(&scope),
            Arc::clone(&self.core),
        ));
        execution.dispatch(OperationJob::Async(Box::pin(future)));
        Ok(handle)
    }
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
    pub fn new(
        options: crate::v02::report::ManagerOptions,
    ) -> Result<Self, crate::v02::report::ManagerCreateError> {
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
                        .wait_for_retry_capacity(
                            &self.target,
                            ExecutionKind::Async,
                            options.clone(),
                        )
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
            let body = lock(&body_for_job)
                .take()
                .expect("accepted async body is present");
            let (operation, slot, lease, parent, control) = lock(&parts_for_job)
                .take()
                .expect("accepted async runner capsule is present");
            let scope_core = Arc::clone(&scope.core);
            let operation_id = scope.id();
            async move {
                let frame = ExecutionFrameGuard::new(Arc::clone(&scope_core));
                operation.set_execution(ExecutionPhase::Running);
                let manager_core = scope_core
                    .manager()
                    .upgrade()
                    .expect("active scope manager exists");
                let groups = scope_core.groups().to_vec();
                let construction = {
                    let _context =
                        ExecutionContextGuard::enter(&manager_core, operation_id, &groups);
                    catch_unwind(AssertUnwindSafe(|| body(scope)))
                };
                let result = match construction {
                    Ok(user_future) => {
                        ExecutionContextFuture::new(user_future, manager_core, operation_id, groups)
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
                    let body = lock(&body_cell)
                        .take()
                        .expect("rejected async body is retained");
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
        let future = runner(OperationScope::new(
            Arc::clone(&scope),
            Arc::clone(&self.core),
        ));
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
        Ok(crate::v02::report::OperationCompletion {
            outcome,
            settlement,
        })
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

#[cfg(test)]
mod production_pending_tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use super::AsyncOperationManager;
    use crate::v02::report::ManagerOptions;
    use crate::v02::report::OperationOptions;
    use crate::v02::report::OperationOutcome;
    use crate::v02::report::ShutdownMode;
    use crate::v02::report::ShutdownWait;

    struct PendingWithoutSavedWaker {
        first_poll: Option<mpsc::Sender<()>>,
        drops: Arc<AtomicUsize>,
    }

    impl Future for PendingWithoutSavedWaker {
        type Output = Result<(), ()>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            if let Some(first_poll) = self.get_mut().first_poll.take() {
                first_poll.send(()).unwrap();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }

    impl Drop for PendingWithoutSavedWaker {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_production_manager_retains_pending_operation_without_saved_waker() {
        let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
        let drops = Arc::new(AtomicUsize::new(0));
        let (first_poll, polled) = mpsc::channel();
        let mut operation = manager
            .try_submit(OperationOptions::default(), {
                let drops = Arc::clone(&drops);
                move |_| PendingWithoutSavedWaker {
                    first_poll: Some(first_poll),
                    drops,
                }
            })
            .unwrap()
            .into_sync();

        polled
            .recv_timeout(Duration::from_secs(3))
            .expect("the production manager should poll the async operation");
        let task_waker = manager.core.test_active_async_task_waker();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        task_waker
            .expect("the production executor should retain the pending task")
            .wake();

        assert!(matches!(
            operation.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(())
        ));
        assert!(operation.wait_settlement().unwrap().all_released());
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(matches!(
            manager
                .as_sync()
                .request_shutdown(ShutdownMode::Drain)
                .wait_for(Duration::from_secs(3))
                .unwrap(),
            ShutdownWait::Completed(_)
        ));
    }
}

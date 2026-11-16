// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use super::AdmissionError;
use super::AsyncOperationManager;
use super::ChildRelease;
use super::DiagnosticSummary;
use super::ExecutionFrameGuard;
use super::ExecutionKind;
use super::ExecutionPhase;
use super::ExternalExecutionContext;
use super::ExternalOutcomePublisher;
use super::ExternalStopControl;
use super::ExternalSubmissionState;
use super::ManagedOperationControl;
use super::ManagerCore;
use super::OperationCore;
use super::OperationHandle;
use super::OperationId;
use super::OperationJob;
use super::OperationOptions;
use super::OperationOutcome;
use super::OperationScope;
use super::OutcomeSlot;
use super::PanicSummary;
use super::RunnerParts;
use super::ScopeCore;
use super::StopControl;
use super::SubmissionTarget;
use super::SubmitError;
use super::block_on;
use super::child_outcome_summary;
use super::drive_external;
use super::lock;
use super::panic_message;
use crate::v02::protocol::OperationControl;
use crate::v02::scope::ExecutionContextGuard;

/// A synchronous submission view over a shared manager coordinator.
///
/// Each view keeps one submission target, so admission and resource limits
/// remain bound to the root manager or group that created it.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationManager;
/// use qubit_operation::ManagerOptions;
///
/// let manager = OperationManager::new(ManagerOptions::default()).unwrap();
/// manager.close_admission();
/// ```
#[must_use = "a manager view is required to submit and coordinate operations"]
#[derive(Clone)]
pub struct OperationManager {
    /// Shared coordinator that owns manager state and operation lifecycles.
    core: Arc<ManagerCore>,
    /// Target that fixes the root or parent group used by submissions.
    pub(super) target: SubmissionTarget,
}
impl OperationManager {
    /// Returns a view constrained by the supplied service group.
    ///
    /// The returned view retains its parent target and applies the group's
    /// admission boundary to future submissions.
    ///
    /// # Parameters
    ///
    /// - `self`: Manager view from which the constrained view is created.
    /// - `group`: Service group that constrains future submissions.
    ///
    /// # Returns
    ///
    /// A view bound to the supplied group, or a binding error if the group
    /// cannot be attached to this manager view.
    pub fn with_group(
        &self,
        group: &crate::v02::manager::group::OperationGroup,
    ) -> Result<Self, crate::v02::report::GroupBindingError> {
        if !Arc::ptr_eq(&self.core, &group.manager_core()) {
            return Err(crate::v02::report::GroupBindingError::WrongManager);
        }
        if let SubmissionTarget::Parent { scope, .. } = &self.target {
            let Some(scope) = scope.upgrade() else {
                return Err(crate::v02::report::GroupBindingError::ScopeEnded);
            };
            if !scope.is_open() {
                return Err(crate::v02::report::GroupBindingError::ScopeEnded);
            }
        }
        if !group.snapshot().accepting {
            return Err(crate::v02::report::GroupBindingError::Closed);
        }
        let existing = match &self.target {
            SubmissionTarget::Root { groups } | SubmissionTarget::Parent { groups, .. } => groups,
        };
        if existing.iter().any(|item| Arc::ptr_eq(item, &group.group_core())) {
            return Ok(self.clone());
        }
        if existing.len() >= self.core.max_groups_per_operation {
            return Err(crate::v02::report::GroupBindingError::CapacityExceeded);
        }
        Ok(self.clone().with_group_core(group.group_core()))
    }

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
    /// - `prepared`: Task that the manager binds and drives synchronously.
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
    ) -> Result<OperationHandle<T, E>, SubmitError<Arc<P>>>
    where
        T: Send + 'static,
        E: Send + 'static,
        P: crate::v02::adapter::PreparedExternalOperation<T, E>,
    {
        let bind_result: Arc<Mutex<Option<Result<(), crate::v02::report::ExecutionFailure>>>> =
            Arc::new(Mutex::new(None));
        let managed_control: Arc<Mutex<Option<Arc<ManagedOperationControl>>>> = Arc::new(Mutex::new(None));
        let execution_context: Arc<Mutex<Option<Arc<ExternalExecutionContext>>>> = Arc::new(Mutex::new(None));
        let outcome_publisher: ExternalOutcomePublisher<T, E> = Arc::new(Mutex::new(None));
        let activation_permitted = Arc::new(AtomicBool::new(false));
        let state_for_preflight = Arc::clone(&bind_result);
        let adapter_for_preflight = Arc::clone(&prepared);
        let preflight = move |id| {
            let result = panic::catch_unwind(AssertUnwindSafe(|| adapter_for_preflight.bind(id)));
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
        let state_for_runner = Arc::clone(&bind_result);
        let managed_for_runner = Arc::clone(&managed_control);
        let execution_for_runner = Arc::clone(&execution_context);
        let publisher_for_runner = Arc::clone(&outcome_publisher);
        let activation_for_runner = Arc::clone(&activation_permitted);
        let adapter_for_runner = Arc::clone(&prepared);
        let runner = move |scope: OperationScope| {
            block_on(drive_external(
                scope,
                adapter_for_runner,
                state_for_runner,
                managed_for_runner,
                execution_for_runner,
                publisher_for_runner,
                activation_for_runner.load(Ordering::Acquire),
            ));
            None
        };
        let domain: Arc<dyn OperationControl> = Arc::clone(&prepared) as Arc<dyn OperationControl>;
        let adapter_for_abandon = Arc::clone(&prepared);
        let abandon = Box::new(move || {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| adapter_for_abandon.abandon()));
        });
        match self.try_submit_raw_with_preflight(
            options,
            runner,
            preflight,
            ExternalSubmissionState {
                external_control: Some(domain),
                confirm_input_release: false,
                on_abandon: Some(abandon),
                external_managed: Some(Arc::clone(&managed_control)),
                external_execution: Some(Arc::clone(&execution_context)),
                early_publisher: Some(Arc::clone(&outcome_publisher)),
                activation_permitted: Some(Arc::clone(&activation_permitted)),
            },
        ) {
            Ok(handle) => Ok(handle),
            Err(error) => {
                let (reason, rejected_runner) = error.into_parts();
                drop(rejected_runner);
                Err(SubmitError::new(reason, prepared))
            }
        }
    }
    /// Constructs a manager with real bounded executors and resource cleanup.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission, executor, and cleanup settings for the manager.
    ///
    /// # Returns
    ///
    /// A synchronous manager view over the created coordinator.
    ///
    /// # Errors
    ///
    /// Returns `ManagerCreateError` if the configured production resources
    /// cannot be initialized.
    pub fn new(options: crate::v02::report::ManagerOptions) -> Result<Self, crate::v02::report::ManagerCreateError> {
        crate::v02::manager::production::new_sync(options)
    }

    /// Creates a view from an existing coordinator and target.
    ///
    /// Cloning the coordinator handle keeps shared state alive, while the
    /// supplied target preserves the caller's admission scope.
    ///
    /// # Parameters
    ///
    /// - `core`: Coordinator shared by this and related manager views.
    /// - `target`: Root or parent submission scope used by this view.
    ///
    /// # Returns
    ///
    /// A manager view bound to the supplied target.
    #[inline]
    pub(super) fn from_parts(core: Arc<ManagerCore>, target: SubmissionTarget) -> Self {
        Self { core, target }
    }

    /// Creates a sync view whose submissions are children of `scope`.
    ///
    /// The weak parent link in the manager target does not extend the scope's
    /// lifetime; submissions fail once the parent scope has ended.
    ///
    /// # Parameters
    ///
    /// - `core`: Shared coordinator used to create the bound view.
    /// - `scope`: Parent operation scope that receives future submissions.
    ///
    /// # Returns
    ///
    /// A manager view that submits under the given scope.
    #[inline]
    pub(crate) fn bound(core: Arc<ManagerCore>, scope: &Arc<ScopeCore>) -> Self {
        core.bound(scope)
    }

    /// Closes admission while allowing already accepted operations to drain.
    ///
    /// This is equivalent to requesting
    /// [`ShutdownMode::Drain`](crate::v02::report::ShutdownMode::Drain).
    ///
    /// The call stops new submissions while accepted work continues to drain.
    #[inline]
    pub fn close_admission(&self) {
        self.core.request_shutdown(crate::v02::report::ShutdownMode::Drain);
    }

    /// Requests shutdown and returns a handle for observing its completion.
    ///
    /// `Drain` stops new submissions and waits for accepted work; `Cancel` also
    /// requests cancellation of work that has not completed.
    ///
    /// # Parameters
    ///
    /// - `mode`: Whether to drain accepted operations or request cancellation.
    ///
    /// # Returns
    ///
    /// A handle for observing when shutdown and cleanup have completed.
    #[inline]
    pub fn request_shutdown(
        &self,
        mode: crate::v02::report::ShutdownMode,
    ) -> crate::v02::manager::shutdown::ShutdownHandle {
        self.core.request_shutdown(mode);
        crate::v02::manager::shutdown::ShutdownHandle::new(Arc::clone(&self.core))
    }

    /// Creates a group view that applies the supplied limits to its operations.
    ///
    /// # Parameters
    ///
    /// - `options`: Capacity and resource-scope limits assigned to the group.
    ///
    /// # Returns
    ///
    /// A group manager whose submissions use those limits.
    ///
    /// # Errors
    ///
    /// Returns a creation error when the group options are invalid or the
    /// manager can no longer admit a group.
    pub fn group(
        &self,
        options: crate::v02::report::GroupOptions,
    ) -> Result<crate::v02::manager::group::OperationGroup, crate::v02::report::ManagerCreateError> {
        crate::v02::manager::group::OperationGroup::new(self.clone(), options)
    }

    /// Returns a sync view with the same fixed submission target.
    ///
    /// # Returns
    ///
    /// A clone sharing the same coordinator and submission target.
    #[inline]
    pub fn as_sync(&self) -> Self {
        self.clone()
    }

    /// Returns the corresponding async view without changing its target.
    ///
    /// # Returns
    ///
    /// An async manager sharing this coordinator and fixed submission target.
    #[inline]
    pub fn as_async(&self) -> AsyncOperationManager {
        AsyncOperationManager::from_parts(Arc::clone(&self.core), self.target.clone())
    }

    /// Registers a service resource scope with the capacity belonging to this
    /// view.
    ///
    /// The scope is attached to the same root or parent group used by
    /// operation submissions, so its resource admission follows that target.
    ///
    /// # Parameters
    ///
    /// - `capacity`: Maximum concurrent resource scopes for this manager view.
    ///
    /// # Returns
    ///
    /// The registered service resource scope.
    ///
    /// # Errors
    ///
    /// Returns `ResourceScopeError` when scope registration is rejected.
    pub(crate) fn service_resource_scope(
        &self,
        capacity: usize,
    ) -> Result<crate::v02::resource::service::ServiceResourceScope, crate::v02::report::ResourceScopeError> {
        self.core
            .service_resource_scope(
                capacity,
                match &self.target {
                    SubmissionTarget::Root { groups } | SubmissionTarget::Parent { groups, .. } => groups.clone(),
                },
            )
            .map(crate::v02::resource::service::ServiceResourceScope::new)
    }

    /// Clones the shared coordinator handle for internal resource integration.
    ///
    /// # Returns
    ///
    /// An `Arc` keeping the shared coordinator alive for internal integration.
    #[inline]
    pub(crate) fn core_arc(&self) -> Arc<ManagerCore> {
        Arc::clone(&self.core)
    }

    /// Returns a view routed through the supplied group policy.
    ///
    /// # Parameters
    ///
    /// - `group`: Shared policy applied to later submissions through the view.
    ///
    /// # Returns
    ///
    /// This manager view with its target updated to use `group`.
    #[inline]
    pub(crate) fn with_group_core(mut self, group: Arc<crate::v02::manager::group::GroupCore>) -> Self {
        match &mut self.target {
            SubmissionTarget::Root { groups } | SubmissionTarget::Parent { groups, .. } => {
                if !groups.iter().any(|item| Arc::ptr_eq(item, &group)) {
                    groups.push(group);
                }
            }
        }
        self
    }

    /// Submits a synchronous operation, waiting for retryable capacity
    /// pressure in its group, manager, or selected executor.
    ///
    /// The body is retained and retried only when admission reports `Busy`;
    /// other admission failures return it to the caller in `SubmitError`.
    /// Waiting is bounded by the manager's admission-waiter limit and does not
    /// guarantee FIFO ordering. Root submissions are rejected after manager
    /// Drain begins, while descendants of accepted scopes may continue.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Value produced by the operation body.
    /// - `E`: Error produced by the operation body.
    /// - `F`: One-shot closure that receives the operation scope.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission, resource, and execution settings for the
    ///   operation.
    /// - `body`: Synchronous body that returns the operation result.
    ///
    /// # Returns
    ///
    /// A handle observing outcome and settlement, or the original body on
    /// rejection.
    ///
    /// # Errors
    ///
    /// Returns the original body with the admission or operation setup error.
    pub fn submit<T, E, F>(&self, options: OperationOptions, body: F) -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
    {
        let mut body = Some(body);
        loop {
            match self.try_submit(
                options.clone(),
                body.take().expect("submission body remains available until accepted"),
            ) {
                Ok(handle) => return Ok(handle),
                Err(error) => {
                    let (reason, original) = error.into_parts();
                    if reason != AdmissionError::Busy {
                        return Err(SubmitError::new(reason, original));
                    }
                    if let Err(reason) = block_on(self.core.wait_for_retry_capacity(
                        &self.target,
                        ExecutionKind::Sync,
                        options.clone(),
                    )) {
                        return Err(SubmitError::new(reason, original));
                    }
                    body = Some(original);
                }
            }
        }
    }

    /// Submits a body through the shared operation-tree coordinator.
    /// Attempts one synchronous admission without waiting for capacity.
    ///
    /// If admission fails, the original body is returned in `SubmitError`.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Value produced by the operation body.
    /// - `E`: Error produced by the operation body.
    /// - `F`: One-shot closure that receives the operation scope.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission, resource, and execution settings for the
    ///   operation.
    /// - `body`: Synchronous body that returns the operation result.
    ///
    /// # Returns
    ///
    /// A handle for the accepted operation or its original body and rejection
    /// reason.
    ///
    /// # Errors
    ///
    /// Returns the original body and admission error when the operation cannot
    /// be accepted immediately.
    pub fn try_submit<T, E, F>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
    {
        let original = Arc::new(Mutex::new(Some(body)));
        let original_for_runner = Arc::clone(&original);
        let runner = move |scope: OperationScope| {
            let body = lock(&original_for_runner)
                .take()
                .expect("accepted body remains available until execution");
            let scope_core = Arc::clone(&scope.core);
            Some(scope_core.resolve_outcome(body(scope)))
        };
        match self.try_submit_raw_with_preflight(
            options,
            runner,
            |_| Ok(()),
            ExternalSubmissionState {
                external_control: None,
                confirm_input_release: true,
                on_abandon: None,
                external_managed: None,
                external_execution: None,
                early_publisher: None,
                activation_permitted: None,
            },
        ) {
            Ok(handle) => Ok(handle),
            Err(error) => {
                let (reason, rejected_runner) = error.into_parts();
                drop(rejected_runner);
                let body = lock(&original)
                    .take()
                    .expect("rejected submission retains original body");
                Err(SubmitError::new(reason, body))
            }
        }
    }

    /// Submits a body that directly produces the operation's public outcome.
    /// The preflight callback runs after admission and before parent/group
    /// registration.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Successful value produced by the operation.
    /// - `E`: Error value produced by the operation.
    /// - `F`: Body that returns an optional typed operation outcome.
    /// - `C`: One-shot callback that performs admission preflight.
    ///
    /// # Parameters
    ///
    /// - `self`: Manager view submitting the operation.
    /// - `options`: Admission and scope settings for the operation.
    /// - `body`: Operation body retained until dispatch.
    /// - `preflight`: Identity claim run after admission succeeds.
    /// - `external_state`: Optional protocol control, release, and lifecycle
    ///   state for the submitted operation.
    ///
    /// # Returns
    ///
    /// The accepted operation handle, or the admission error with the body.
    ///
    /// # Errors
    ///
    /// Returns the body with the admission error when the operation is
    /// rejected.
    fn try_submit_raw_with_preflight<T, E, F, C>(
        &self,
        options: OperationOptions,
        body: F,
        preflight: C,
        external_state: ExternalSubmissionState<T, E>,
    ) -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Option<OperationOutcome<T, E>> + Send + 'static,
        C: FnOnce(OperationId) -> Result<(), AdmissionError>,
    {
        let ExternalSubmissionState {
            external_control,
            confirm_input_release,
            on_abandon,
            external_managed,
            external_execution,
            early_publisher,
            activation_permitted,
        } = external_state;
        let body_cell = Arc::new(Mutex::new(Some(body)));
        let body_for_job = Arc::clone(&body_cell);
        let early_published = Arc::new(AtomicBool::new(false));
        let early_published_for_runner = Arc::clone(&early_published);
        let parts: Arc<Mutex<Option<RunnerParts<T, E>>>> = Arc::new(Mutex::new(None));
        let parts_for_job = Arc::clone(&parts);
        let external_mode = external_execution.is_some();
        let external_execution_for_runner = external_execution.clone();
        let runner = move |scope: OperationScope| {
            let body = lock(&body_for_job).take().expect("accepted body is present");
            let (operation, slot, lease, parent, control) =
                lock(&parts_for_job).take().expect("accepted runner capsule is present");
            let scope_core = Arc::clone(&scope.core);
            let operation_id = scope.id();
            let mut frame = Some(ExecutionFrameGuard::new(Arc::clone(&scope_core)));
            operation.set_execution(ExecutionPhase::Running);
            if let Some(external_execution) = &external_execution_for_runner {
                *lock(external_execution) = Some(Arc::new(ExternalExecutionContext::new(
                    Arc::clone(&operation),
                    Arc::clone(&lease),
                    Arc::clone(&scope_core),
                    frame.take(),
                )));
            }
            let manager_core = scope_core.manager().upgrade().expect("active scope manager exists");
            let groups = scope_core.groups().to_vec();
            let result = {
                let _context = ExecutionContextGuard::enter(&manager_core, operation_id, &groups);
                panic::catch_unwind(AssertUnwindSafe(|| body(scope)))
            };
            if external_execution.is_none() {
                operation.set_execution(ExecutionPhase::Finished);
                lease.execution_finished();
            } else if let Some(context) = lock(external_execution_for_runner.as_ref().unwrap()).as_ref() {
                context.finish();
            }
            if confirm_input_release {
                control.confirm_input_release(ChildRelease {
                    operation_id,
                    confirmed_at: None,
                });
            }
            if !early_published_for_runner.load(Ordering::Acquire) {
                scope_core.on_outcomes_ready(move || {
                    let outcome = match result {
                        Ok(Some(outcome)) => outcome,
                        Ok(None) => OperationOutcome::ExecutionFailed(crate::v02::report::ExecutionFailure {
                            operation_id,
                            summary: DiagnosticSummary::new("external driver ended without publishing an outcome"),
                            source: None,
                        }),
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
            drop(frame);
            if !external_mode {
                scope_core.mark_body_finished();
            }
        };
        let (runner, id, scope, reservation) = match self.core.reserve_scope_with_preflight(
            &self.target,
            options,
            ExecutionKind::Sync,
            runner,
            preflight,
            external_mode,
        ) {
            Ok(result) => result,
            Err(error) => {
                let (reason, _runner) = error.into_parts();
                let body = lock(&body_cell).take().expect("rejected body is retained");
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
        if let Some(external_managed) = external_managed {
            *lock(&external_managed) = Some(Arc::clone(&control));
        }
        if let Some(early_publisher) = early_publisher {
            let scope_for_publisher = Arc::clone(&scope);
            let control_for_publisher = Arc::clone(&control);
            let slot_for_publisher = Arc::clone(&slot);
            let parent_for_publisher = parent.clone();
            let published_for_callback = Arc::clone(&early_published);
            *lock(&early_publisher) = Some(Box::new(move |outcome| {
                if published_for_callback.swap(true, Ordering::AcqRel) {
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
        }
        if external_mode {
            scope.retain_external_driver_settlement();
        }
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
        let stop: Arc<dyn StopControl> = match external_control {
            Some(_) => Arc::new(ExternalStopControl {
                managed: Arc::clone(&control),
            }),
            None => Arc::clone(&control) as Arc<dyn StopControl>,
        };
        let handle = OperationHandle::new(Arc::clone(&operation), Arc::clone(&slot), stop);
        let core = Arc::clone(&self.core);
        let abandon_scope = Arc::clone(&scope);
        let abandon_operation = Arc::clone(&operation);
        let abandon_slot = Arc::clone(&slot);
        let abandon_lease = Arc::clone(&lease);
        let abandon_parent = scope.parent();
        let abandon_control = Arc::clone(&control);
        let mut abandon_external = on_abandon;
        let execution = reservation.execution;
        if external_mode {
            let start_permitted = scope.commit_and_activate_start().is_ok();
            activation_permitted
                .as_ref()
                .expect("external submissions have an activation decision")
                .store(start_permitted, Ordering::Release);
            let _ = abandon_external.take();
            execution.dispatch(OperationJob::Sync(Box::new(move || {
                runner(OperationScope::new(scope, core));
            })));
        } else {
            let _ = self.core.dispatch_adapter_job(
                Arc::clone(&scope),
                Arc::clone(&control),
                execution,
                OperationJob::Sync(Box::new(move || {
                    runner(OperationScope::new(scope, core));
                })),
                move || {
                    if let Some(abandon) = abandon_external.take() {
                        abandon();
                    }
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
        }
        Ok(handle)
    }

    /// Submits a synchronous operation and waits for its outcome.
    ///
    /// This returns when the outcome is available; resource settlement may
    /// still be in progress.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Value produced by the operation body.
    /// - `E`: Error produced by the operation body.
    /// - `F`: One-shot closure that receives the operation scope.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission and resource limits for the operation.
    /// - `body`: Synchronous body that returns the operation result.
    ///
    /// # Returns
    ///
    /// The operation outcome when it becomes available.
    ///
    /// # Errors
    ///
    /// Returns submission or outcome-observation failure, retaining the body
    /// when submission is rejected.
    pub fn run<T, E, F>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationOutcome<T, E>, crate::v02::report::RunError<F>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
    {
        let mut operation = self
            .submit(options, body)
            .map_err(crate::v02::report::RunError::Submit)?;
        operation.wait_outcome().map_err(crate::v02::report::RunError::Outcome)
    }

    /// Submits a synchronous operation and waits for both outcome and
    /// settlement.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Value produced by the operation body.
    /// - `E`: Error produced by the operation body.
    /// - `F`: One-shot closure that receives the operation scope.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission and resource limits for the operation.
    /// - `body`: Synchronous body that returns the operation result.
    ///
    /// # Returns
    ///
    /// The outcome and settlement report after both are observable.
    ///
    /// # Errors
    ///
    /// Returns submission, outcome-observation, or settlement-observation
    /// failure, retaining the body when submission is rejected.
    pub fn run_and_settle<T, E, F>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<crate::v02::report::OperationCompletion<T, E>, crate::v02::report::RunError<F>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
    {
        let mut operation = self
            .submit(options, body)
            .map_err(crate::v02::report::RunError::Submit)?;
        let outcome = operation
            .wait_outcome()
            .map_err(crate::v02::report::RunError::Outcome)?;
        let settlement = operation
            .wait_settlement()
            .map_err(crate::v02::report::RunError::Observation)?;
        Ok(crate::v02::report::OperationCompletion { outcome, settlement })
    }

    /// Submits a unit-returning body through the test execution port.
    ///
    /// # Type Parameters
    ///
    /// - `F`: One-shot operation body type.
    ///
    /// # Parameters
    ///
    /// - `body`: Unit-returning operation body to submit.
    #[cfg(test)]
    pub(crate) fn try_submit_body<F>(&self, body: F) -> Result<OperationId, SubmitError<F>>
    where
        F: FnOnce(OperationScope) + Send + 'static,
    {
        self.core.submit_scope(&self.target, OperationOptions::default(), body)
    }

    /// Submits a configured unit-returning body through the test port.
    ///
    /// # Type Parameters
    ///
    /// - `F`: One-shot operation body type.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission and resource options for the test operation.
    /// - `body`: Unit-returning operation body to submit.
    #[cfg(test)]
    pub(crate) fn try_submit_body_with<F>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationId, SubmitError<F>>
    where
        F: FnOnce(OperationScope) + Send + 'static,
    {
        self.core.submit_scope(&self.target, options, body)
    }
}

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

use super::AdmissionError;
use super::AsyncOperationManager;
use super::ChildRelease;
use super::DiagnosticSummary;
use super::ExecutionFrameGuard;
use super::ExecutionKind;
use super::ExecutionPhase;
use super::ManagedOperationControl;
use super::ManagerCore;
use super::OperationCore;
use super::OperationHandle;
#[cfg(test)]
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
use super::lock;
use super::panic_message;
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
    pub fn new(
        options: crate::v02::report::ManagerOptions,
    ) -> Result<Self, crate::v02::report::ManagerCreateError> {
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
        self.core
            .request_shutdown(crate::v02::report::ShutdownMode::Drain);
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
    ) -> Result<crate::v02::manager::group::OperationGroup, crate::v02::report::ManagerCreateError>
    {
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
    ) -> Result<
        crate::v02::resource::service::ServiceResourceScope,
        crate::v02::report::ResourceScopeError,
    > {
        self.core
            .service_resource_scope(
                capacity,
                match &self.target {
                    SubmissionTarget::Root { group } | SubmissionTarget::Parent { group, .. } => {
                        group.clone()
                    }
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
    pub(crate) fn with_group(mut self, group: Arc<crate::v02::manager::group::GroupCore>) -> Self {
        match &mut self.target {
            SubmissionTarget::Root {
                group: target_group,
            } => *target_group = Some(group),
            SubmissionTarget::Parent {
                group: target_group,
                ..
            } => *target_group = Some(group),
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
    pub fn submit<T, E, F>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
    {
        let mut body = Some(body);
        loop {
            match self.try_submit(
                options.clone(),
                body.take()
                    .expect("submission body remains available until accepted"),
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
        let body_cell = Arc::new(Mutex::new(Some(body)));
        let body_for_job = Arc::clone(&body_cell);
        let parts: Arc<Mutex<Option<RunnerParts<T, E>>>> = Arc::new(Mutex::new(None));
        let parts_for_job = Arc::clone(&parts);
        let runner = move |scope: OperationScope| {
            let body = lock(&body_for_job)
                .take()
                .expect("accepted body is present");
            let (operation, slot, lease, parent, control) = lock(&parts_for_job)
                .take()
                .expect("accepted runner capsule is present");
            let scope_core = Arc::clone(&scope.core);
            let operation_id = scope.id();
            let frame = ExecutionFrameGuard::new(Arc::clone(&scope_core));
            operation.set_execution(ExecutionPhase::Running);
            let manager_core = scope_core
                .manager()
                .upgrade()
                .expect("active scope manager exists");
            let group = scope_core.group().cloned();
            let result = {
                let _context =
                    ExecutionContextGuard::enter(&manager_core, operation_id, group.as_ref());
                panic::catch_unwind(AssertUnwindSafe(|| body(scope)))
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
            drop(frame);
            scope_core.mark_body_finished();
        };
        let (runner, id, scope, reservation) =
            match self
                .core
                .reserve_scope(&self.target, options, ExecutionKind::Sync, runner)
            {
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
        let handle = OperationHandle::new(
            Arc::clone(&operation),
            Arc::clone(&slot),
            Arc::clone(&control) as Arc<dyn StopControl>,
        );
        let core = Arc::clone(&self.core);
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
            OperationJob::Sync(Box::new(move || {
                runner(OperationScope::new(scope, core));
            })),
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
        operation
            .wait_outcome()
            .map_err(crate::v02::report::RunError::Outcome)
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
        Ok(crate::v02::report::OperationCompletion {
            outcome,
            settlement,
        })
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
        self.core
            .submit_scope(&self.target, OperationOptions::default(), body)
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

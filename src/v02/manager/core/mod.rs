//! Shared admission and operation-tree coordination for manager views.

use crate::v02::context::OperationContext;
use crate::v02::operation::OutcomeDelivery;
use crate::v02::operation::{
    AsyncOperationHandle, OperationCore, OperationHandle, OutcomeSlot, StopControl,
};
use crate::v02::report::{
    AdmissionError, ChildOutcomeKind, ChildOutcomeSummary, ContextError, ContextOptions,
    DiagnosticSummary, ExecutionPhase, OperationId, OperationOptions, OperationOutcome,
    PanicSummary, ScopeLimits, SettlementReport, SettlementStatus, StopReason, SubmitError,
};
use crate::v02::scope::{ExecutionFrameGuard, OperationScope, ScopeCore};
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::Poll;

/// The immutable submission location carried by manager views.
#[derive(Clone)]
pub(crate) enum SubmissionTarget {
    Root,
    Parent {
        id: OperationId,
        scope: Weak<ScopeCore>,
    },
}

/// Pre-reserved, infallible handoff to an execution engine.
pub(crate) trait SubmissionPermit: Send + 'static {
    fn dispatch(self: Box<Self>, job: OperationJob);
}

/// Capacity permits whose release points follow operation lifecycle facts.
pub(crate) trait SubmissionLease: Send + Sync + 'static {
    fn execution_finished(&self);
    fn settlement_finished(&self);
    fn outcome_delivered(&self, delivery: OutcomeDelivery);
}

/// The complete, reversible reservation made before tree registration.
pub(crate) struct SubmissionReservation {
    pub(crate) execution: Box<dyn SubmissionPermit>,
    pub(crate) lifecycle: Arc<dyn SubmissionLease>,
}

/// A sync closure or async future whose frame guard ends only on completion/drop.
pub(crate) enum OperationJob {
    Sync(Box<dyn FnOnce() + Send + 'static>),
    Async(Pin<Box<dyn Future<Output = ()> + Send + 'static>>),
}

/// Capacity and execution boundary supplied by the production manager assembly.
pub(crate) trait SubmissionPort: Send + Sync + 'static {
    fn reserve(
        &self,
        parent: Option<&ScopeCore>,
        options: &OperationOptions,
    ) -> Result<SubmissionReservation, AdmissionError>;

    /// Waits for a retryable capacity miss. Implementations must reserve a
    /// bounded waiter slot and wake for capacity, manager close, or parent
    /// scope end/stop. Dropping the future must unregister and release it.
    fn wait_for_capacity<'a>(
        &'a self,
        parent: Option<Arc<ScopeCore>>,
        options: OperationOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), AdmissionError>> + Send + 'a>>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManagerPhase {
    Open,
    Draining,
    Cancelling,
    Closed,
}

struct ManagerState {
    phase: ManagerPhase,
    #[cfg(test)]
    relationships: Vec<(OperationId, Option<OperationId>)>,
    #[cfg(test)]
    values: Vec<usize>,
}

/// State shared by all sync and async manager views.
pub(crate) struct ManagerCore {
    state: Mutex<ManagerState>,
    port: Arc<dyn SubmissionPort>,
    root_context: Arc<OperationContext>,
    scope_limits: ScopeLimits,
    diagnostic_limit: usize,
}

impl ManagerCore {
    /// Creates the real coordinator around supplied capacity and execution ports.
    pub(crate) fn new(
        port: Arc<dyn SubmissionPort>,
        root_context: OperationContext,
        scope_limits: ScopeLimits,
    ) -> Arc<Self> {
        Self::new_with_diagnostic_limit(port, root_context, scope_limits, 256)
    }

    pub(crate) fn new_with_diagnostic_limit(
        port: Arc<dyn SubmissionPort>,
        root_context: OperationContext,
        scope_limits: ScopeLimits,
        diagnostic_limit: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ManagerState {
                phase: ManagerPhase::Open,
                #[cfg(test)]
                relationships: Vec::new(),
                #[cfg(test)]
                values: Vec::new(),
            }),
            port,
            root_context: Arc::new(root_context),
            scope_limits,
            diagnostic_limit,
        })
    }

    pub(crate) fn root_view(self: &Arc<Self>) -> OperationManager {
        OperationManager {
            core: Arc::clone(self),
            target: SubmissionTarget::Root,
        }
    }

    pub(crate) fn bound(self: Arc<Self>, scope: &Arc<ScopeCore>) -> OperationManager {
        OperationManager {
            core: self,
            target: SubmissionTarget::Parent {
                id: scope.id(),
                scope: Arc::downgrade(scope),
            },
        }
    }

    fn reserve_scope<F>(
        self: &Arc<Self>,
        target: &SubmissionTarget,
        mut options: OperationOptions,
        body: F,
    ) -> Result<(F, OperationId, Arc<ScopeCore>, SubmissionReservation), SubmitError<F>>
    where
        F: Send + 'static,
    {
        let parent = match target {
            SubmissionTarget::Root => None,
            SubmissionTarget::Parent { id, scope } => {
                let Some(scope) = scope.upgrade() else {
                    return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
                };
                if !scope.is_open() {
                    return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
                }
                if let Err(error) = scope.context().check() {
                    return Err(SubmitError::new(map_context_error(error), body));
                }
                Some((*id, scope))
            }
        };
        if options.limits == ScopeLimits::default() {
            options.limits = self.scope_limits;
        }
        let context = match parent.as_ref() {
            Some((_, scope)) => scope.context().child(options.context.clone()),
            None => self.root_context.child(options.context.clone()),
        };
        let context = match context {
            Ok(context) => context,
            Err(error) => return Err(SubmitError::new(map_context_error(error), body)),
        };
        if let Err(error) = context.check() {
            return Err(SubmitError::new(map_context_error(error), body));
        }
        let parent_id = parent.as_ref().map(|(id, _)| *id);
        let permit = match self
            .port
            .reserve(parent.as_ref().map(|(_, scope)| scope.as_ref()), &options)
        {
            Ok(permit) => permit,
            Err(reason) => return Err(SubmitError::new(reason, body)),
        };
        let id = OperationId::new_unique();
        let scope_core = ScopeCore::new(
            id,
            Arc::downgrade(self),
            parent.as_ref().map(|(_, scope)| Arc::downgrade(scope)),
            parent_id,
            context,
            options,
            self.diagnostic_limit,
        );
        {
            let mut state = lock(&self.state);
            let allowed = match (state.phase, parent_id) {
                (ManagerPhase::Open, _) => true,
                (ManagerPhase::Draining, Some(_)) => true,
                (ManagerPhase::Draining, None) => false,
                (ManagerPhase::Cancelling | ManagerPhase::Closed, _) => false,
            };
            if !allowed {
                drop(state);
                return Err(SubmitError::new(
                    if parent_id.is_some() {
                        AdmissionError::Stopped
                    } else {
                        AdmissionError::Closed
                    },
                    body,
                ));
            }
            if let Some((_, parent_scope)) = parent.as_ref()
                && !parent_scope.mark_child(
                    id,
                    Arc::downgrade(&scope_core),
                    scope_core.options().requirement,
                )
            {
                drop(state);
                return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
            }
            #[cfg(test)]
            state.relationships.push((id, parent_id));
        }
        Ok((body, id, scope_core, permit))
    }

    fn submit_scope<F>(
        self: &Arc<Self>,
        target: &SubmissionTarget,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationId, SubmitError<F>>
    where
        F: FnOnce(OperationScope) + Send + 'static,
    {
        let (body, id, scope_core, permit) = self.reserve_scope(target, options, body)?;
        let core = Arc::clone(self);
        permit
            .execution
            .dispatch(OperationJob::Sync(Box::new(move || {
                let scope = OperationScope::new(Arc::clone(&scope_core), core);
                let _frame = ExecutionFrameGuard::new(Arc::clone(&scope_core));
                body(scope);
            })));
        Ok(id)
    }

    fn submit_async_scope<F, Fut>(
        self: &Arc<Self>,
        target: &SubmissionTarget,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationId, SubmitError<F>>
    where
        F: FnOnce(OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let (body, id, scope_core, permit) = self.reserve_scope(target, options, body)?;
        let core = Arc::clone(self);
        let scope = OperationScope::new(Arc::clone(&scope_core), core);
        let future = async move {
            let _frame = ExecutionFrameGuard::new(scope_core);
            body(scope).await;
        };
        permit
            .execution
            .dispatch(OperationJob::Async(Box::pin(future)));
        Ok(id)
    }

    fn make_settlement_report(&self, scope: &ScopeCore) -> SettlementReport {
        let (children, children_total, children_truncated) = scope.child_reports();
        let (
            resources,
            resource_reports_total,
            resource_reports_truncated,
            cleanup_failures,
            cleanup_failures_total,
            cleanup_failures_truncated,
            recoveries,
            recoveries_total,
            recoveries_truncated,
            transfers,
            transfers_total,
            transfers_truncated,
        ) = scope.resource_reports();
        let status = if cleanup_failures_total != 0 || recoveries_total != 0 {
            SettlementStatus::RecoveryRequired
        } else if transfers_total != 0 {
            SettlementStatus::Transferred
        } else {
            SettlementStatus::Released
        };
        SettlementReport {
            operation_id: scope.id(),
            status,
            released_count: resource_reports_total,
            transferred_count: transfers_total,
            recovery_count: recoveries_total,
            children: Arc::from(children),
            children_total,
            children_truncated,
            cleanup_failures_total,
            cleanup_failures_truncated,
            cleanup_failures: Arc::from(cleanup_failures),
            resource_reports_total,
            resource_reports_truncated,
            resource_reports: Arc::from(resources),
            transfers: Arc::from(transfers),
            transfers_total,
            transfers_truncated,
            recoveries: Arc::from(recoveries),
            recoveries_total,
            recoveries_truncated,
            effects: Arc::from([]),
            effects_total: 0,
            effect_overflow: 0,
        }
    }

    fn register_settlement(
        self: &Arc<Self>,
        scope: Arc<ScopeCore>,
        operation: Arc<OperationCore>,
        lease: Arc<dyn SubmissionLease>,
    ) {
        let parent = scope.parent();
        let operation_id = scope.id();
        let manager = Arc::clone(self);
        let scope_for_callback = Arc::clone(&scope);
        scope.on_settlement_ready(move || {
            let report = Arc::new(manager.make_settlement_report(&scope_for_callback));
            // Return the exact settlement reservation before making the
            // completed report observable to waiters.
            lease.settlement_finished();
            if operation.publish_settlement((*report).clone()).is_ok() {
                if let Some(parent) = parent {
                    parent.record_child_settlement(operation_id, report);
                }
            }
        });
    }

    async fn wait_for_retry_capacity(
        &self,
        parent: Option<Arc<ScopeCore>>,
        options: OperationOptions,
    ) -> Result<(), AdmissionError> {
        let port_wait = self.port.wait_for_capacity(parent.clone(), options);
        let Some(parent) = parent else {
            return port_wait.await;
        };
        let mut port_wait = port_wait;
        let mut scope_wait = Box::pin(parent.wait_unavailable());
        std::future::poll_fn(|context| {
            if let Poll::Ready(reason) = scope_wait.as_mut().poll(context) {
                return Poll::Ready(Err(reason));
            }
            if let Poll::Ready(result) = port_wait.as_mut().poll(context) {
                return Poll::Ready(result);
            }
            Poll::Pending
        })
        .await
    }

    #[cfg(test)]
    pub(crate) fn record_test_value(&self, value: usize) {
        lock(&self.state).values.push(value);
    }

    #[cfg(test)]
    fn test_relationships(&self) -> Vec<(OperationId, Option<OperationId>)> {
        lock(&self.state).relationships.clone()
    }

    #[cfg(test)]
    fn test_values(&self) -> Vec<usize> {
        lock(&self.state).values.clone()
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> (Arc<Self>, OperationManager) {
        Self::for_test_with_diagnostic_limit(256)
    }

    #[cfg(test)]
    pub(crate) fn for_test_with_diagnostic_limit(
        diagnostic_limit: usize,
    ) -> (Arc<Self>, OperationManager) {
        let core = Self::new_with_diagnostic_limit(
            Arc::new(TestPort),
            OperationContext::new(ContextOptions::default()).unwrap(),
            ScopeLimits::default(),
            diagnostic_limit,
        );
        let root = core.root_view();
        (core, root)
    }
}

/// A sync view over the shared coordinator and one fixed target.
#[derive(Clone)]
pub struct OperationManager {
    core: Arc<ManagerCore>,
    target: SubmissionTarget,
}
impl OperationManager {
    pub(crate) fn bound(core: Arc<ManagerCore>, scope: &Arc<ScopeCore>) -> Self {
        core.bound(scope)
    }

    /// Returns a sync view with the same fixed submission target.
    pub fn as_sync(&self) -> Self {
        self.clone()
    }

    /// Returns the corresponding async view without changing its target.
    pub fn as_async(&self) -> AsyncOperationManager {
        AsyncOperationManager {
            core: Arc::clone(&self.core),
            target: self.target.clone(),
        }
    }

    pub(crate) fn submit<T, E, F>(
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
                    let parent = match resolve_wait_parent(&self.target) {
                        Ok(parent) => parent,
                        Err(reason) => return Err(SubmitError::new(reason, original)),
                    };
                    if let Err(reason) =
                        block_on(self.core.wait_for_retry_capacity(parent, options.clone()))
                    {
                        return Err(SubmitError::new(reason, original));
                    }
                    body = Some(original);
                }
            }
        }
    }

    /// Submits a body through the shared operation-tree coordinator.
    pub(crate) fn try_submit<T, E, F>(
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
            let (operation, slot, lease, parent) = lock(&parts_for_job)
                .take()
                .expect("accepted runner capsule is present");
            let scope_core = Arc::clone(&scope.core);
            let operation_id = scope.id();
            let frame = ExecutionFrameGuard::new(Arc::clone(&scope_core));
            let result = panic::catch_unwind(AssertUnwindSafe(|| body(scope)));
            operation.set_execution(ExecutionPhase::Finished);
            lease.execution_finished();
            if let Some(parent) = &parent {
                parent.child_input_released(operation_id);
            }
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
                if let Some(parent) = parent {
                    let _ =
                        parent.record_child_outcome(child_outcome_summary(operation_id, &outcome));
                }
                let _ = slot.publish(outcome);
            });
            drop(frame);
            scope_core.mark_body_finished();
        };
        let (runner, id, scope, reservation) =
            match self.core.reserve_scope(&self.target, options, runner) {
                Ok(result) => result,
                Err(error) => {
                    let (reason, _runner) = error.into_parts();
                    let body = lock(&body_cell).take().expect("rejected body is retained");
                    return Err(SubmitError::new(reason, body));
                }
            };
        let operation = OperationCore::new(id);
        operation.set_execution(ExecutionPhase::Running);
        let slot = Arc::new(OutcomeSlot::new(Arc::clone(&operation)));
        let lease = Arc::clone(&reservation.lifecycle);
        slot.set_delivery_hook({
            let lease = Arc::clone(&lease);
            move |delivery| lease.outcome_delivered(delivery)
        });
        let parent = scope.parent();
        self.core.register_settlement(
            Arc::clone(&scope),
            Arc::clone(&operation),
            Arc::clone(&lease),
        );
        *lock(&parts) = Some((
            Arc::clone(&operation),
            Arc::clone(&slot),
            Arc::clone(&lease),
            parent,
        ));
        let handle = OperationHandle::new(
            operation,
            slot,
            Arc::new(ScopeStopControl(Arc::downgrade(&scope))),
        );
        let core = Arc::clone(&self.core);
        reservation
            .execution
            .dispatch(OperationJob::Sync(Box::new(move || {
                runner(OperationScope::new(scope, core));
            })));
        Ok(handle)
    }

    pub(crate) fn try_submit_with<T, E, F>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        T: Send + 'static,
        E: Send + 'static,
        F: FnOnce(OperationScope) -> Result<T, E> + Send + 'static,
    {
        self.try_submit(options, body)
    }

    #[cfg(test)]
    pub(crate) fn try_submit_body<F>(&self, body: F) -> Result<OperationId, SubmitError<F>>
    where
        F: FnOnce(OperationScope) + Send + 'static,
    {
        self.core
            .submit_scope(&self.target, OperationOptions::default(), body)
    }

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

/// An async view over the shared coordinator and one fixed target.
#[derive(Clone)]
pub struct AsyncOperationManager {
    core: Arc<ManagerCore>,
    target: SubmissionTarget,
}
impl AsyncOperationManager {
    /// Returns the corresponding sync view without changing its target.
    pub fn as_sync(&self) -> OperationManager {
        OperationManager {
            core: Arc::clone(&self.core),
            target: self.target.clone(),
        }
    }

    pub(crate) async fn submit<T, E, F, Fut>(
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
                    let parent = match resolve_wait_parent(&self.target) {
                        Ok(parent) => parent,
                        Err(reason) => return Err(SubmitError::new(reason, original)),
                    };
                    if let Err(reason) = self
                        .core
                        .wait_for_retry_capacity(parent, options.clone())
                        .await
                    {
                        return Err(SubmitError::new(reason, original));
                    }
                    body = Some(original);
                }
            }
        }
    }

    pub(crate) fn try_submit<T, E, F, Fut>(
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
            let (operation, slot, lease, parent) = lock(&parts_for_job)
                .take()
                .expect("accepted async runner capsule is present");
            let scope_core = Arc::clone(&scope.core);
            let operation_id = scope.id();
            async move {
                let frame = ExecutionFrameGuard::new(Arc::clone(&scope_core));
                let result = CatchUnwindFuture(Box::pin(body(scope))).await;
                operation.set_execution(ExecutionPhase::Finished);
                lease.execution_finished();
                if let Some(parent) = &parent {
                    parent.child_input_released(operation_id);
                }
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
                    if let Some(parent) = parent {
                        let _ = parent
                            .record_child_outcome(child_outcome_summary(operation_id, &outcome));
                    }
                    let _ = slot.publish(outcome);
                });
                drop(frame);
                scope_core.mark_body_finished();
            }
        };
        let (runner, id, scope, reservation) =
            match self.core.reserve_scope(&self.target, options, runner) {
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
        operation.set_execution(ExecutionPhase::Running);
        let slot = Arc::new(OutcomeSlot::new(Arc::clone(&operation)));
        let lease = Arc::clone(&reservation.lifecycle);
        slot.set_delivery_hook({
            let lease = Arc::clone(&lease);
            move |delivery| lease.outcome_delivered(delivery)
        });
        let parent = scope.parent();
        self.core.register_settlement(
            Arc::clone(&scope),
            Arc::clone(&operation),
            Arc::clone(&lease),
        );
        *lock(&parts) = Some((
            Arc::clone(&operation),
            Arc::clone(&slot),
            Arc::clone(&lease),
            parent,
        ));
        let handle = AsyncOperationHandle::new(
            operation,
            slot,
            Arc::new(ScopeStopControl(Arc::downgrade(&scope))),
        );
        let future = runner(OperationScope::new(scope, Arc::clone(&self.core)));
        reservation
            .execution
            .dispatch(OperationJob::Async(Box::pin(future)));
        Ok(handle)
    }

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

type RunnerParts<T, E> = (
    Arc<OperationCore>,
    Arc<OutcomeSlot<T, E>>,
    Arc<dyn SubmissionLease>,
    Option<Arc<ScopeCore>>,
);

struct CatchUnwindFuture<F>(Pin<Box<F>>);
impl<F: Future> Future for CatchUnwindFuture<F> {
    type Output = Result<F::Output, Box<dyn std::any::Any + Send>>;

    fn poll(self: Pin<&mut Self>, context: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let future = self.get_mut();
        match panic::catch_unwind(AssertUnwindSafe(|| future.0.as_mut().poll(context))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

struct ScopeStopControl(Weak<ScopeCore>);
impl StopControl for ScopeStopControl {
    fn request_stop(&self, reason: StopReason) {
        if let Some(scope) = self.0.upgrade() {
            scope.request_stop(reason);
        }
    }
}

fn child_outcome_summary<T, E>(
    operation_id: OperationId,
    outcome: &OperationOutcome<T, E>,
) -> ChildOutcomeSummary {
    let (kind, summary) = match outcome {
        OperationOutcome::Succeeded(_) => (ChildOutcomeKind::Succeeded, "child succeeded"),
        OperationOutcome::Failed(_) | OperationOutcome::ChildFailed(_) => {
            (ChildOutcomeKind::Failed, "child failed")
        }
        OperationOutcome::Stopped(_) => (ChildOutcomeKind::Stopped, "child stopped"),
        OperationOutcome::Panicked(panic) => (ChildOutcomeKind::Panicked, panic.summary.as_str()),
        OperationOutcome::ExecutionFailed(failure) => {
            (ChildOutcomeKind::ExecutionFailed, failure.summary.as_str())
        }
    };
    ChildOutcomeSummary {
        operation_id,
        kind,
        summary: DiagnosticSummary::new(summary),
    }
}

fn resolve_wait_parent(
    target: &SubmissionTarget,
) -> Result<Option<Arc<ScopeCore>>, AdmissionError> {
    let SubmissionTarget::Parent { scope, .. } = target else {
        return Ok(None);
    };
    let scope = scope.upgrade().ok_or(AdmissionError::ScopeEnded)?;
    if !scope.is_open() {
        return Err(AdmissionError::ScopeEnded);
    }
    if let Err(error) = scope.context().check() {
        return Err(map_context_error(error));
    }
    Ok(Some(scope))
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        format!("operation panicked: {message}")
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        format!("operation panicked: {message}")
    } else {
        "operation panicked with a non-string payload".to_owned()
    }
}

struct ThreadUnparker(std::thread::Thread);
impl std::task::Wake for ThreadUnparker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let unparker = Arc::new(ThreadUnparker(std::thread::current()));
    let waker = std::task::Waker::from(unparker);
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

fn map_context_error(error: ContextError) -> AdmissionError {
    match error {
        ContextError::Stopped => AdmissionError::Stopped,
        ContextError::ClockUnavailable | ContextError::ClockDomainMismatch => {
            AdmissionError::DeadlineExpired
        }
        ContextError::BudgetRejected(_) => AdmissionError::CapacityExceeded,
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
struct TestPort;
#[cfg(test)]
struct TestPermit;
#[cfg(test)]
struct TestLease;
#[cfg(test)]
impl SubmissionPort for TestPort {
    fn reserve(
        &self,
        _parent: Option<&ScopeCore>,
        _options: &OperationOptions,
    ) -> Result<SubmissionReservation, AdmissionError> {
        Ok(SubmissionReservation {
            execution: Box::new(TestPermit),
            lifecycle: Arc::new(TestLease),
        })
    }

    fn wait_for_capacity<'a>(
        &'a self,
        _parent: Option<Arc<ScopeCore>>,
        _options: OperationOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), AdmissionError>> + Send + 'a>> {
        Box::pin(std::future::pending())
    }
}
#[cfg(test)]
impl SubmissionLease for TestLease {
    fn execution_finished(&self) {}

    fn settlement_finished(&self) {}

    fn outcome_delivered(&self, _delivery: OutcomeDelivery) {}
}
#[cfg(test)]
impl SubmissionPermit for TestPermit {
    fn dispatch(self: Box<Self>, job: OperationJob) {
        match job {
            OperationJob::Sync(job) => job(),
            OperationJob::Async(future) => poll_immediately(future),
        }
    }
}

#[cfg(test)]
fn poll_immediately(mut future: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
    let waker = std::task::Waker::noop();
    let mut context = std::task::Context::from_waker(waker);
    loop {
        if future.as_mut().poll(&mut context).is_ready() {
            return;
        }
        std::thread::yield_now();
    }
}

#[cfg(test)]
mod tests;

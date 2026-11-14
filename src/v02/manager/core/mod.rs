//! Shared admission and operation-tree coordination for manager views.

use crate::v02::adapter::{AdapterErrorKind, AdapterReservation, ManagedOperationControl};
use crate::v02::context::OperationContext;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::operation::OutcomeDelivery;
use crate::v02::operation::{
    AsyncOperationHandle, OperationCore, OperationHandle, OutcomeSlot, StopControl,
};
use crate::v02::report::{
    AdmissionError, ChildOutcomeKind, ChildOutcomeSummary, ChildRelease, ContextError,
    DiagnosticSummary, ExecutionPhase, OperationId, OperationOptions, OperationOutcome,
    PanicSummary, ScopeLimits, SettlementReport, SettlementStatus, StopReason, StopReport,
    SubmitError,
};
use crate::v02::resource::ResourceScope;
use crate::v02::scope::{ExecutionFrameGuard, OperationScope, ScopeCore};
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::task::Poll;

/// The immutable submission location carried by manager views.
#[derive(Clone)]
pub(crate) enum SubmissionTarget {
    Root {
        group: Option<Arc<super::group::GroupCore>>,
    },
    Parent {
        id: OperationId,
        scope: Weak<ScopeCore>,
        group: Option<Arc<super::group::GroupCore>>,
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
    pub(crate) capacity: Arc<ScopeCapacity>,
    pub(crate) resource_scope: ResourceScope,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionKind {
    Sync,
    Async,
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
        scope_id: OperationId,
        execution: ExecutionKind,
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

    fn phase_changed(&self, _phase: ManagerPhase) {}
    fn finish_shutdown(&self) {}
    fn queued_operation_count(&self) -> usize {
        0
    }
    fn service_resource_scope(
        &self,
        _capacity: usize,
    ) -> Result<ResourceScope, crate::v02::report::ResourceScopeError> {
        Err(crate::v02::report::ResourceScopeError::InvalidOptions)
    }
    fn operation_resource_scope(
        &self,
        _id: crate::v02::report::ResourceScopeId,
        _capacity: Arc<ScopeCapacity>,
    ) -> Result<ResourceScope, crate::v02::report::ResourceScopeError> {
        Err(crate::v02::report::ResourceScopeError::InvalidOptions)
    }
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
    active_scopes: std::collections::HashMap<OperationId, Weak<ScopeCore>>,
    service_scopes: std::collections::HashMap<crate::v02::report::ResourceScopeId, ResourceScope>,
    recoveries: std::collections::HashMap<
        crate::v02::report::RecoveryId,
        crate::v02::report::RecoveryHandle,
    >,
    #[cfg(test)]
    relationships: Vec<(OperationId, Option<OperationId>)>,
    #[cfg(test)]
    values: Vec<usize>,
}

/// State shared by all sync and async manager views.
pub(crate) struct ManagerCore {
    state: Mutex<ManagerState>,
    lifecycle_gate: Mutex<()>,
    shutdown_changed: Condvar,
    shutdown_notify: crate::v02::internal::notify::Notify,
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
                active_scopes: std::collections::HashMap::new(),
                service_scopes: std::collections::HashMap::new(),
                recoveries: std::collections::HashMap::new(),
                #[cfg(test)]
                relationships: Vec::new(),
                #[cfg(test)]
                values: Vec::new(),
            }),
            lifecycle_gate: Mutex::new(()),
            shutdown_changed: Condvar::new(),
            shutdown_notify: crate::v02::internal::notify::Notify::new(),
            port,
            root_context: Arc::new(root_context),
            scope_limits,
            diagnostic_limit,
        })
    }

    /// Serializes scope stops with adapter commit and activation transitions.
    pub(crate) fn with_lifecycle_gate<R>(&self, action: impl FnOnce() -> R) -> R {
        let _gate = lock(&self.lifecycle_gate);
        action()
    }

    pub(crate) fn adapter_start_allowed(&self) -> bool {
        matches!(
            lock(&self.state).phase,
            ManagerPhase::Open | ManagerPhase::Draining
        )
    }

    pub(crate) fn root_view(self: &Arc<Self>) -> OperationManager {
        OperationManager {
            core: Arc::clone(self),
            target: SubmissionTarget::Root { group: None },
        }
    }

    pub(crate) fn bound(self: Arc<Self>, scope: &Arc<ScopeCore>) -> OperationManager {
        OperationManager {
            core: self,
            target: SubmissionTarget::Parent {
                id: scope.id(),
                scope: Arc::downgrade(scope),
                group: scope.group().cloned(),
            },
        }
    }

    fn reserve_scope<F>(
        self: &Arc<Self>,
        target: &SubmissionTarget,
        mut options: OperationOptions,
        execution: ExecutionKind,
        body: F,
    ) -> Result<(F, OperationId, Arc<ScopeCore>, SubmissionReservation), SubmitError<F>>
    where
        F: Send + 'static,
    {
        let (parent, group) = match target {
            SubmissionTarget::Root { group } => (None, group.as_ref()),
            SubmissionTarget::Parent { id, scope, group } => {
                let Some(scope) = scope.upgrade() else {
                    return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
                };
                if !scope.is_open() {
                    return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
                }
                if let Err(error) = scope.context().check() {
                    return Err(SubmitError::new(map_context_error(error), body));
                }
                (Some((*id, scope)), group.as_ref())
            }
        };
        let group_permit = match group {
            Some(group) => match group.reserve(parent.is_some()) {
                Ok(permit) => Some(permit),
                Err(reason) => return Err(SubmitError::new(reason, body)),
            },
            None => None,
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
        let id = OperationId::new_unique();
        let permit = match self.port.reserve(
            id,
            execution,
            parent.as_ref().map(|(_, scope)| scope.as_ref()),
            &options,
        ) {
            Ok(permit) => permit,
            Err(reason) => return Err(SubmitError::new(reason, body)),
        };
        self.track_resource_recoveries(&permit.resource_scope, group.cloned());
        let scope_core = ScopeCore::new(
            id,
            Arc::downgrade(self),
            parent.as_ref().map(|(_, scope)| Arc::downgrade(scope)),
            parent_id,
            context,
            options,
            self.diagnostic_limit,
            Arc::clone(&permit.capacity),
            permit.resource_scope.internal_clone(),
            group.cloned(),
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
            if let Some(group) = group
                && !group.register_scope(id, Arc::downgrade(&scope_core))
            {
                drop(state);
                return Err(SubmitError::new(AdmissionError::Stopped, body));
            }
            if let Some((_, parent_scope)) = parent.as_ref()
                && !parent_scope.mark_child(
                    id,
                    Arc::downgrade(&scope_core),
                    scope_core.options().requirement,
                )
            {
                if let Some(group) = group {
                    group.unregister_scope(id);
                }
                drop(state);
                return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
            }
            state.active_scopes.insert(id, Arc::downgrade(&scope_core));
            #[cfg(test)]
            state.relationships.push((id, parent_id));
        }
        let mut permit = permit;
        if let Some(group_permit) = group_permit {
            permit.lifecycle = Arc::new(super::group::GroupLease::new(
                Arc::clone(&permit.lifecycle),
                group_permit,
            ));
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
        let (body, id, scope_core, permit) =
            self.reserve_scope(target, options, ExecutionKind::Sync, body)?;
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
        let (body, id, scope_core, permit) =
            self.reserve_scope(target, options, ExecutionKind::Async, body)?;
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
        control: Arc<ManagedOperationControl>,
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
                control.settlement_published();
                if let Some(parent) = parent {
                    parent.record_child_settlement(operation_id, report);
                }
            }
            let mut state = lock(&manager.state);
            state.active_scopes.remove(&operation_id);
            drop(state);
            if let Some(group) = scope_for_callback.group() {
                group.settlement_finished(operation_id);
            }
            manager.shutdown_changed.notify_all();
            manager.shutdown_notify.notify_waiters();
        });
    }

    fn dispatch_adapter_job(
        self: &Arc<Self>,
        scope: Arc<ScopeCore>,
        control: Arc<ManagedOperationControl>,
        execution: Box<dyn SubmissionPermit>,
        job: OperationJob,
        abandon: impl FnOnce() + Send + 'static,
    ) -> Result<(), AdapterErrorKind> {
        let commit_scope = Arc::clone(&scope);
        let activate_scope = Arc::clone(&scope);
        let adapter = AdapterReservation::new_with_gates(
            Box::new(()),
            move || commit_scope.commit_start(),
            move || activate_scope.activate_start(),
            move |payload| {
                let Ok(job) = payload.downcast::<OperationJob>() else {
                    return;
                };
                execution.dispatch(*job);
            },
            abandon,
        );
        let child = match adapter.attach(control, job) {
            Ok(child) => child,
            Err(error) => return Err(error.reason),
        };
        let permit = match child.commit() {
            Ok(permit) => permit,
            Err(error) => {
                let reason = error.reason;
                drop(error.payload);
                return Err(reason);
            }
        };
        match permit.activate() {
            Ok(()) => Ok(()),
            Err(error) => {
                let reason = error.reason;
                drop(error.payload);
                Err(reason)
            }
        }
    }

    fn abandon_reserved_operation<T: Send + 'static, E: Send + 'static>(
        scope: Arc<ScopeCore>,
        operation: Arc<OperationCore>,
        slot: Arc<OutcomeSlot<T, E>>,
        lease: Arc<dyn SubmissionLease>,
        parent: Option<Arc<ScopeCore>>,
        control: Arc<ManagedOperationControl>,
    ) {
        scope.abandon_start();
        let operation_id = operation.id();
        operation.set_execution(ExecutionPhase::Finished);
        lease.execution_finished();
        control.confirm_input_release(ChildRelease {
            operation_id,
            confirmed_at: None,
        });
        let callback_scope = Arc::clone(&scope);
        scope.on_outcomes_ready(move || {
            let outcome = OperationOutcome::Stopped(StopReport {
                reason: callback_scope
                    .context()
                    .stop_reason()
                    .unwrap_or(StopReason::ParentStopped),
                accepted_at: None,
            });
            let summary = child_outcome_summary(operation_id, &outcome);
            control.publish_outcome(summary.clone());
            if let Some(parent) = parent {
                let _ = parent.record_child_outcome(summary);
            }
            let _ = slot.publish(outcome);
        });
        scope.end();
        scope.mark_body_finished();
    }

    pub(crate) fn request_shutdown(&self, mode: crate::v02::report::ShutdownMode) -> ManagerPhase {
        let (phase, scopes, services) = self.with_lifecycle_gate(|| {
            let mut state = lock(&self.state);
            match (state.phase, mode) {
                (ManagerPhase::Open, crate::v02::report::ShutdownMode::Drain) => {
                    state.phase = ManagerPhase::Draining;
                }
                (
                    ManagerPhase::Open | ManagerPhase::Draining,
                    crate::v02::report::ShutdownMode::Cancel,
                ) => {
                    state.phase = ManagerPhase::Cancelling;
                }
                _ => {}
            }
            let scopes = if state.phase == ManagerPhase::Cancelling {
                state
                    .active_scopes
                    .values()
                    .filter_map(Weak::upgrade)
                    .collect()
            } else {
                Vec::new()
            };
            let services = state
                .service_scopes
                .values()
                .map(ResourceScope::internal_clone)
                .collect::<Vec<_>>();
            (state.phase, scopes, services)
        });
        self.port.phase_changed(phase);
        if phase == ManagerPhase::Cancelling {
            for scope in scopes {
                scope.request_stop(StopReason::ManagerShutdown);
            }
        }
        for service in services {
            service.end();
        }
        phase
    }

    pub(crate) fn phase(&self) -> ManagerPhase {
        lock(&self.state).phase
    }

    pub(crate) fn clock(&self) -> Option<Arc<dyn crate::v02::protocol::ClockSource>> {
        self.root_context.clock_source()
    }

    pub(crate) fn active_scope_count(&self) -> usize {
        lock(&self.state).active_scopes.len()
    }

    pub(crate) fn finish_shutdown(&self) {
        let is_empty = {
            let state = lock(&self.state);
            state.active_scopes.is_empty() && state.service_scopes.is_empty()
        };
        if is_empty {
            self.port.finish_shutdown();
            lock(&self.state).phase = ManagerPhase::Closed;
        }
    }

    pub(crate) fn service_resource_scope(
        self: &Arc<Self>,
        capacity: usize,
        group: Option<Arc<super::group::GroupCore>>,
    ) -> Result<ResourceScope, crate::v02::report::ResourceScopeError> {
        let scope = self.port.service_resource_scope(capacity)?;
        self.track_resource_recoveries(&scope, group.clone());
        let id = scope.id();
        let manager = Arc::downgrade(self);
        let group_callback = group.clone();
        scope.set_settlement_callback(Box::new(move |_| {
            let removed = if let Some(manager) = manager.upgrade() {
                let removed = lock(&manager.state).service_scopes.remove(&id);
                manager.shutdown_changed.notify_all();
                manager.shutdown_notify.notify_waiters();
                removed
            } else {
                None
            };
            if let Some(group) = group_callback {
                group.service_finished(id);
            }
            drop(removed);
        }));
        {
            let mut state = lock(&self.state);
            if state.phase != ManagerPhase::Open {
                drop(state);
                scope.end();
                return Err(crate::v02::report::ResourceScopeError::ScopeEnded);
            }
            state.service_scopes.insert(id, scope.internal_clone());
        }
        if let Some(group) = group {
            if !group.register_service(scope.internal_clone()) {
                scope.end();
                return Err(crate::v02::report::ResourceScopeError::ScopeEnded);
            }
        }
        Ok(scope)
    }

    pub(crate) fn operation_resource_scope(
        self: &Arc<Self>,
        owner: &Arc<ScopeCore>,
        options: crate::v02::report::ResourceScopeOptions,
    ) -> Result<ResourceScope, crate::v02::report::ResourceScopeError> {
        if options.capacity == 0 {
            return Err(crate::v02::report::ResourceScopeError::InvalidOptions);
        }
        if !owner.is_open() {
            return Err(crate::v02::report::ResourceScopeError::ScopeEnded);
        }
        let id = crate::v02::report::ResourceScopeId::new_unique();
        let capacity = Arc::new(owner.capacity().resource_scope_for(id, options.capacity));
        let resource_scope = self.port.operation_resource_scope(id, capacity)?;
        self.track_resource_recoveries(&resource_scope, owner.group().cloned());
        match owner.attach_resource_scope(resource_scope.internal_clone()) {
            Ok(()) => Ok(resource_scope),
            Err(resource_scope) => {
                resource_scope.end();
                Err(crate::v02::report::ResourceScopeError::ScopeEnded)
            }
        }
    }

    pub(crate) fn shutdown_wait_for(&self, timeout: std::time::Duration) -> bool {
        let until = std::time::Instant::now() + timeout;
        let mut state = lock(&self.state);
        while !state.active_scopes.is_empty()
            || !state.service_scopes.is_empty()
            || !state.recoveries.is_empty()
        {
            let now = std::time::Instant::now();
            if now >= until {
                return false;
            }
            let (next, result) = self
                .shutdown_changed
                .wait_timeout(state, until.saturating_duration_since(now))
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state = next;
            if result.timed_out()
                && (!state.active_scopes.is_empty()
                    || !state.service_scopes.is_empty()
                    || !state.recoveries.is_empty())
            {
                return false;
            }
        }
        drop(state);
        self.finish_shutdown();
        true
    }

    pub(crate) fn shutdown_wait_blocking(&self) {
        let mut state = lock(&self.state);
        while !state.active_scopes.is_empty()
            || !state.service_scopes.is_empty()
            || !state.recoveries.is_empty()
        {
            state = self
                .shutdown_changed
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        drop(state);
        self.finish_shutdown();
    }

    pub(crate) async fn shutdown_wait(&self) {
        loop {
            if self.is_shutdown_empty() {
                self.finish_shutdown();
                return;
            }
            let changed = self.shutdown_notify.notified();
            if self.is_shutdown_empty() {
                continue;
            }
            changed.await;
        }
    }

    pub(crate) fn is_shutdown_empty(&self) -> bool {
        let state = lock(&self.state);
        state.active_scopes.is_empty()
            && state.service_scopes.is_empty()
            && state.recoveries.is_empty()
    }

    pub(crate) fn service_scope_count(&self) -> usize {
        lock(&self.state).service_scopes.len()
    }

    pub(crate) fn recovery_group_count(&self) -> usize {
        lock(&self.state).recoveries.len()
    }

    pub(crate) fn unconfirmed_recovery_count(&self) -> usize {
        let handles = lock(&self.state)
            .recoveries
            .values()
            .cloned()
            .collect::<Vec<_>>();
        handles
            .iter()
            .filter(|handle| {
                handle.snapshot().phase == crate::v02::report::RecoveryPhase::Unconfirmed
            })
            .count()
    }

    pub(crate) fn queued_operation_count(&self) -> usize {
        self.port.queued_operation_count()
    }

    pub(crate) fn cleaning_resource_count(&self) -> usize {
        let (operations, services) = {
            let state = lock(&self.state);
            (
                state
                    .active_scopes
                    .values()
                    .filter_map(Weak::upgrade)
                    .collect::<Vec<_>>(),
                state
                    .service_scopes
                    .values()
                    .map(ResourceScope::internal_clone)
                    .collect::<Vec<_>>(),
            )
        };
        operations
            .iter()
            .map(|scope| scope.cleanup_in_flight())
            .sum::<usize>()
            + services
                .iter()
                .map(ResourceScope::cleanup_in_flight)
                .sum::<usize>()
    }

    fn track_resource_recoveries(
        self: &Arc<Self>,
        scope: &ResourceScope,
        group: Option<Arc<super::group::GroupCore>>,
    ) {
        let manager = Arc::downgrade(self);
        let group_callback = group.as_ref().map(Arc::downgrade);
        scope.set_recovery_callback(Arc::new(
            move |handle: crate::v02::report::RecoveryHandle, active: bool| {
                if let Some(group) = group_callback.as_ref().and_then(Weak::upgrade) {
                    group.recovery_changed(handle.clone(), active);
                }
                let Some(manager) = manager.upgrade() else {
                    return;
                };
                let id = handle.id();
                let removed = {
                    let mut state = lock(&manager.state);
                    if active {
                        state.recoveries.insert(id, handle)
                    } else {
                        state.recoveries.remove(&id)
                    }
                };
                drop(removed);
                manager.shutdown_changed.notify_all();
                manager.shutdown_notify.notify_waiters();
            },
        ));
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
            OperationContext::new(crate::v02::report::ContextOptions::default()).unwrap(),
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
    /// Constructs a manager with real bounded executors and resource cleanup.
    pub fn new(
        options: crate::v02::report::ManagerOptions,
    ) -> Result<Self, crate::v02::report::ManagerCreateError> {
        super::production::new_sync(options)
    }

    pub fn close_admission(&self) {
        self.core
            .request_shutdown(crate::v02::report::ShutdownMode::Drain);
    }

    pub fn request_shutdown(
        &self,
        mode: crate::v02::report::ShutdownMode,
    ) -> crate::v02::manager::shutdown::ShutdownHandle {
        self.core.request_shutdown(mode);
        crate::v02::manager::shutdown::ShutdownHandle::new(Arc::clone(&self.core))
    }

    pub fn group(
        &self,
        options: crate::v02::report::GroupOptions,
    ) -> Result<super::group::OperationGroup, crate::v02::report::ManagerCreateError> {
        super::group::OperationGroup::new(self.clone(), options)
    }

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

    pub(crate) fn operation_resource_scope(
        &self,
        owner: &Arc<ScopeCore>,
        options: crate::v02::report::ResourceScopeOptions,
    ) -> Result<ResourceScope, crate::v02::report::ResourceScopeError> {
        self.core.operation_resource_scope(owner, options)
    }

    pub(crate) fn bound(core: Arc<ManagerCore>, scope: &Arc<ScopeCore>) -> Self {
        core.bound(scope)
    }

    pub(crate) fn core_arc(&self) -> Arc<ManagerCore> {
        Arc::clone(&self.core)
    }

    pub(crate) fn with_group(mut self, group: Arc<super::group::GroupCore>) -> Self {
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
            let result = panic::catch_unwind(AssertUnwindSafe(|| body(scope)));
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
    /// Constructs an async view with real bounded executors and cleanup.
    pub fn new(
        options: crate::v02::report::ManagerOptions,
    ) -> Result<Self, crate::v02::report::ManagerCreateError> {
        super::production::new_async(options)
    }

    pub fn close_admission(&self) {
        self.as_sync().close_admission();
    }

    pub fn request_shutdown(
        &self,
        mode: crate::v02::report::ShutdownMode,
    ) -> crate::v02::manager::shutdown::AsyncShutdownHandle {
        self.as_sync().request_shutdown(mode).as_async()
    }

    /// Returns the corresponding sync view without changing its target.
    pub fn as_sync(&self) -> OperationManager {
        OperationManager {
            core: Arc::clone(&self.core),
            target: self.target.clone(),
        }
    }

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
                let result = CatchUnwindFuture(Box::pin(body(scope))).await;
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
    Arc<ManagedOperationControl>,
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
        scope_id: OperationId,
        _execution: ExecutionKind,
        parent: Option<&ScopeCore>,
        options: &OperationOptions,
    ) -> Result<SubmissionReservation, AdmissionError> {
        Ok(test_reservation(scope_id, parent, options))
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
pub(crate) fn test_reservation(
    scope_id: OperationId,
    parent: Option<&ScopeCore>,
    options: &OperationOptions,
) -> SubmissionReservation {
    let (capacity, resource_scope) = test_resources(scope_id, parent, options);
    SubmissionReservation {
        execution: Box::new(TestPermit),
        lifecycle: Arc::new(TestLease),
        capacity,
        resource_scope,
    }
}

#[cfg(test)]
pub(crate) fn test_resources(
    scope_id: OperationId,
    parent: Option<&ScopeCore>,
    options: &OperationOptions,
) -> (
    Arc<crate::v02::manager::capacity::ScopeCapacity>,
    ResourceScope,
) {
    use crate::v02::manager::capacity::{ManagerCapacityPools, ScopeCapacity};
    use crate::v02::report::{ManagerId, ManagerLimits, ResourceScopeId};
    use crate::v02::resource::cleanup_port::{
        CleanupDispatch, CleanupDispatchPermit, CleanupQueueError,
    };

    struct NoCleanup;
    impl CleanupDispatch for NoCleanup {
        fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
            Err(CleanupQueueError::Closed)
        }
    }

    let manager = ManagerCapacityPools::new(ManagerLimits::default());
    let capacity = Arc::new(match parent {
        Some(parent) => parent
            .capacity()
            .child_for_operation(scope_id, options.limits),
        None => ScopeCapacity::root_for_operation(&manager, scope_id, options.limits),
    });
    let resource_scope = ResourceScope::new(
        ManagerId::new_unique(),
        ResourceScopeId::new_unique(),
        Arc::clone(&capacity),
        Arc::new(NoCleanup),
    );
    (capacity, resource_scope)
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

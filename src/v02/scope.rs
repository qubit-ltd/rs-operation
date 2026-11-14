//! Non-cloneable operation scope views and execution-frame lifetime.

use crate::v02::context::OperationContext;
use crate::v02::internal::notify::Notify;
use crate::v02::manager::core::{ManagerCore, OperationManager};
use crate::v02::report::{
    ChildFailurePolicy, ChildFailureSummary, ChildOutcomeKind, ChildOutcomeSummary,
    ChildRequirement, ChildSettlement, CleanupFailure, OperationId, OperationOptions,
    OperationOutcome, RecoveryHandle, ResourceCleanupReport, ResourceScopeReport, SettlementReport,
    StopReason, StopReport, TransferReceipt,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

/// Internal scope state referenced weakly by child-bound manager views.
pub(crate) struct ScopeCore {
    id: OperationId,
    manager: Weak<ManagerCore>,
    parent: Option<Weak<ScopeCore>>,
    parent_id: Option<OperationId>,
    context: OperationContext,
    options: OperationOptions,
    diagnostic_limit: usize,
    children_changed: Notify,
    state: Mutex<ScopeState>,
}

struct ScopeState {
    open: bool,
    stopping: bool,
    active_children: HashMap<OperationId, ChildRecord>,
    children: Vec<ChildSettlement>,
    children_total: usize,
    children_truncated: bool,
    required_failure: Option<ChildFailureSummary>,
    resources: Vec<Box<dyn ScopeResourceOwner>>,
    pending_resource_scopes: usize,
    resource_reports: Vec<ResourceCleanupReport>,
    resource_reports_total: usize,
    resource_reports_truncated: bool,
    cleanup_failures: Vec<CleanupFailure>,
    cleanup_failures_total: usize,
    cleanup_failures_truncated: bool,
    recoveries: Vec<RecoveryHandle>,
    recoveries_total: usize,
    recoveries_truncated: bool,
    transfers: Vec<TransferReceipt>,
    transfers_total: usize,
    transfers_truncated: bool,
    settlement_callback: Option<Box<dyn FnOnce() + Send + 'static>>,
    body_finished: bool,
    outcome_callback: Option<Box<dyn FnOnce() + Send + 'static>>,
}

struct ChildRecord {
    scope: Weak<ScopeCore>,
    requirement: ChildRequirement,
    outcome: Option<ChildOutcomeSummary>,
    settlement: Option<Arc<SettlementReport>>,
    input_released: bool,
}

/// An owner that receives the scope-end event independently of child settlement.
pub(crate) trait ScopeResourceOwner: Send + Sync + 'static {
    fn end(&self);
}

impl ScopeCore {
    pub(crate) fn new(
        id: OperationId,
        manager: Weak<ManagerCore>,
        parent: Option<Weak<ScopeCore>>,
        parent_id: Option<OperationId>,
        context: OperationContext,
        options: OperationOptions,
        diagnostic_limit: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            manager,
            parent,
            parent_id,
            context,
            options,
            diagnostic_limit,
            children_changed: Notify::new(),
            state: Mutex::new(ScopeState {
                open: true,
                stopping: false,
                active_children: HashMap::new(),
                children: Vec::with_capacity(diagnostic_limit),
                children_total: 0,
                children_truncated: false,
                required_failure: None,
                resources: Vec::new(),
                pending_resource_scopes: 0,
                resource_reports: Vec::new(),
                resource_reports_total: 0,
                resource_reports_truncated: false,
                cleanup_failures: Vec::new(),
                cleanup_failures_total: 0,
                cleanup_failures_truncated: false,
                recoveries: Vec::new(),
                recoveries_total: 0,
                recoveries_truncated: false,
                transfers: Vec::new(),
                transfers_total: 0,
                transfers_truncated: false,
                settlement_callback: None,
                body_finished: false,
                outcome_callback: None,
            }),
        })
    }

    pub(crate) fn id(&self) -> OperationId {
        self.id
    }

    pub(crate) fn is_open(&self) -> bool {
        lock(&self.state).open
    }

    pub(crate) fn context(&self) -> &OperationContext {
        &self.context
    }

    pub(crate) fn parent(&self) -> Option<Arc<ScopeCore>> {
        self.parent.as_ref().and_then(Weak::upgrade)
    }

    pub(crate) fn parent_id(&self) -> Option<OperationId> {
        self.parent_id
    }

    pub(crate) fn options(&self) -> &OperationOptions {
        &self.options
    }

    pub(crate) fn mark_child(
        &self,
        id: OperationId,
        scope: Weak<ScopeCore>,
        requirement: ChildRequirement,
    ) -> bool {
        let mut state = lock(&self.state);
        if !state.open || state.stopping || self.context.check().is_err() {
            return false;
        }
        let (children_total, overflowed) = state.children_total.overflowing_add(1);
        state.children_total = if overflowed {
            usize::MAX
        } else {
            children_total
        };
        state.children_truncated |= overflowed;
        state.active_children.insert(
            id,
            ChildRecord {
                scope,
                requirement,
                outcome: None,
                settlement: None,
                input_released: false,
            },
        );
        true
    }

    pub(crate) fn register_resource_owner(
        &self,
        owner: Box<dyn ScopeResourceOwner>,
    ) -> Result<(), Box<dyn ScopeResourceOwner>> {
        let mut state = lock(&self.state);
        if !state.open {
            return Err(owner);
        }
        state.resources.push(owner);
        Ok(())
    }

    pub(crate) fn attach_resource_scope(
        self: &Arc<Self>,
        resource_scope: crate::v02::resource::ResourceScope,
    ) -> Result<(), crate::v02::resource::ResourceScope> {
        {
            let mut state = lock(&self.state);
            if !state.open {
                return Err(resource_scope);
            }
            state.pending_resource_scopes = state.pending_resource_scopes.saturating_add(1);
            state
                .resources
                .push(Box::new(resource_scope.internal_clone()));
        }
        let weak = Arc::downgrade(self);
        resource_scope.set_settlement_callback(Box::new(move |report| {
            if let Some(scope) = weak.upgrade() {
                scope.record_resource_scope_report(report);
            }
        }));
        Ok(())
    }

    fn record_resource_scope_report(&self, report: ResourceScopeReport) {
        let callback = {
            let mut state = lock(&self.state);
            state.pending_resource_scopes = state.pending_resource_scopes.saturating_sub(1);
            state.resource_reports_total = state
                .resource_reports_total
                .saturating_add(report.resources.len());
            for resource in report.resources.iter().cloned() {
                if state.resource_reports.len() < self.diagnostic_limit {
                    state.resource_reports.push(resource);
                } else {
                    state.resource_reports_truncated = true;
                }
            }
            state.cleanup_failures_total = state
                .cleanup_failures_total
                .saturating_add(report.failures.len());
            for failure in report.failures.iter().cloned() {
                if state.cleanup_failures.len() < self.diagnostic_limit {
                    state.cleanup_failures.push(failure);
                } else {
                    state.cleanup_failures_truncated = true;
                }
            }
            state.recoveries_total = state
                .recoveries_total
                .saturating_add(report.recoveries.len());
            for recovery in report.recoveries.iter().cloned() {
                if state.recoveries.len() < self.diagnostic_limit {
                    state.recoveries.push(recovery);
                } else {
                    state.recoveries_truncated = true;
                }
            }
            state.transfers_total = state.transfers_total.saturating_add(report.transfers_total);
            for receipt in report.transfers.iter().cloned() {
                if state.transfers.len() < self.diagnostic_limit {
                    state.transfers.push(receipt);
                } else {
                    state.transfers_truncated = true;
                }
            }
            state.transfers_truncated |= report.transfers_truncated;
            Self::take_settlement_callback(&mut state)
        };
        self.children_changed.notify_waiters();
        invoke_callback(callback);
    }

    pub(crate) fn record_child_outcome(
        &self,
        outcome: ChildOutcomeSummary,
    ) -> Result<(), ChildOutcomeSummary> {
        let id = outcome.operation_id;
        let stop_siblings = {
            let mut state = lock(&self.state);
            let Some(record) = state.active_children.get_mut(&id) else {
                return Err(outcome);
            };
            if record.outcome.is_some() {
                return Err(outcome);
            }
            let failed = outcome.kind != ChildOutcomeKind::Succeeded;
            record.outcome = Some(outcome.clone());
            if failed && record.requirement == ChildRequirement::Required {
                let failure = ChildFailureSummary {
                    operation_id: id,
                    outcome,
                };
                if state.required_failure.is_none() {
                    state.required_failure = Some(failure);
                }
                if self.options.child_failure_policy == ChildFailurePolicy::StopSiblings {
                    state
                        .active_children
                        .iter()
                        .filter(|(child_id, _)| **child_id != id)
                        .filter_map(|(_, sibling)| sibling.scope.upgrade())
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            }
        };
        for sibling in stop_siblings {
            sibling.request_stop(StopReason::RequiredChildFailed);
        }
        let callback = {
            let mut state = lock(&self.state);
            Self::take_outcome_callback(&mut state)
        };
        invoke_callback(callback);
        self.settle_child_if_ready(id);
        Ok(())
    }

    pub(crate) fn mark_body_finished(&self) {
        let callback = {
            let mut state = lock(&self.state);
            state.body_finished = true;
            Self::take_outcome_callback(&mut state)
        };
        invoke_callback(callback);
    }

    pub(crate) fn on_outcomes_ready(&self, callback: impl FnOnce() + Send + 'static) {
        let callback = {
            let mut state = lock(&self.state);
            assert!(state.outcome_callback.is_none());
            if Self::outcomes_ready(&state) {
                Some(Box::new(callback) as Box<dyn FnOnce() + Send>)
            } else {
                state.outcome_callback = Some(Box::new(callback));
                None
            }
        };
        invoke_callback(callback);
    }

    fn outcomes_ready(state: &ScopeState) -> bool {
        state.body_finished
            && state.active_children.values().all(|record| {
                record.requirement != ChildRequirement::Required || record.outcome.is_some()
            })
    }

    fn take_outcome_callback(state: &mut ScopeState) -> Option<Box<dyn FnOnce() + Send>> {
        if Self::outcomes_ready(state) {
            state.outcome_callback.take()
        } else {
            None
        }
    }

    pub(crate) fn record_child_settlement(
        &self,
        id: OperationId,
        settlement: Arc<SettlementReport>,
    ) -> bool {
        {
            let mut state = lock(&self.state);
            let Some(record) = state.active_children.get_mut(&id) else {
                return false;
            };
            if record.settlement.is_some() {
                return false;
            }
            record.settlement = Some(settlement);
        }
        self.settle_child_if_ready(id)
    }

    fn settle_child_if_ready(&self, id: OperationId) -> bool {
        let settled = {
            let mut state = lock(&self.state);
            let ready = state.active_children.get(&id).is_some_and(|record| {
                record.outcome.is_some() && record.settlement.is_some() && record.input_released
            });
            if !ready {
                return false;
            }
            let record = state
                .active_children
                .remove(&id)
                .expect("ready child record exists");
            let child = ChildSettlement {
                operation_id: id,
                outcome: record.outcome.expect("ready child has outcome"),
                settlement: record.settlement.expect("ready child has settlement"),
            };
            if state.children.len() < self.diagnostic_limit {
                state.children.push(child);
            } else {
                state.children_truncated = true;
            }
            true
        };
        if settled {
            self.children_changed.notify_waiters();
            let callback = {
                let mut state = lock(&self.state);
                Self::take_settlement_callback(&mut state)
            };
            invoke_callback(callback);
        }
        settled
    }

    pub(crate) fn required_failure(&self) -> Option<ChildFailureSummary> {
        lock(&self.state).required_failure.clone()
    }

    pub(crate) fn request_stop(&self, reason: StopReason) -> bool {
        {
            let mut state = lock(&self.state);
            if state.stopping {
                return false;
            }
            state.stopping = true;
        }
        let accepted = self.context.request_stop(reason);
        self.children_changed.notify_waiters();
        accepted
    }

    pub(crate) async fn wait_unavailable(&self) -> crate::v02::report::AdmissionError {
        let mut stopped = Box::pin(self.context.stop_requested());
        loop {
            if !self.is_open() {
                return crate::v02::report::AdmissionError::ScopeEnded;
            }
            if self.context.check().is_err() {
                return crate::v02::report::AdmissionError::Stopped;
            }
            let mut changed = Box::pin(self.children_changed.notified());
            let result = std::future::poll_fn(|cx| {
                if !self.is_open() {
                    return std::task::Poll::Ready(Some(
                        crate::v02::report::AdmissionError::ScopeEnded,
                    ));
                }
                if self.context.check().is_err() {
                    return std::task::Poll::Ready(Some(
                        crate::v02::report::AdmissionError::Stopped,
                    ));
                }
                if changed.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(None);
                }
                if stopped.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(Some(
                        crate::v02::report::AdmissionError::Stopped,
                    ));
                }
                std::task::Poll::Pending
            })
            .await;
            if let Some(error) = result {
                return error;
            }
        }
    }

    pub(crate) fn child_reports(&self) -> (Vec<ChildSettlement>, usize, bool) {
        let state = lock(&self.state);
        (
            state.children.clone(),
            state.children_total,
            state.children_truncated,
        )
    }

    pub(crate) fn resource_reports(
        &self,
    ) -> (
        Vec<ResourceCleanupReport>,
        usize,
        bool,
        Vec<CleanupFailure>,
        usize,
        bool,
        Vec<RecoveryHandle>,
        usize,
        bool,
        Vec<TransferReceipt>,
        usize,
        bool,
    ) {
        let state = lock(&self.state);
        (
            state.resource_reports.clone(),
            state.resource_reports_total,
            state.resource_reports_truncated,
            state.cleanup_failures.clone(),
            state.cleanup_failures_total,
            state.cleanup_failures_truncated,
            state.recoveries.clone(),
            state.recoveries_total,
            state.recoveries_truncated,
            state.transfers.clone(),
            state.transfers_total,
            state.transfers_truncated,
        )
    }

    pub(crate) fn children_settled(&self) -> bool {
        lock(&self.state).active_children.is_empty()
    }

    pub(crate) fn settlement_ready(&self) -> bool {
        let state = lock(&self.state);
        !state.open && state.active_children.is_empty() && state.pending_resource_scopes == 0
    }

    pub(crate) fn on_settlement_ready(&self, callback: impl FnOnce() + Send + 'static) {
        let callback = {
            let mut state = lock(&self.state);
            assert!(state.settlement_callback.is_none());
            if !state.open && state.active_children.is_empty() && state.pending_resource_scopes == 0
            {
                Some(Box::new(callback) as Box<dyn FnOnce() + Send>)
            } else {
                state.settlement_callback = Some(Box::new(callback));
                None
            }
        };
        invoke_callback(callback);
    }

    fn take_settlement_callback(state: &mut ScopeState) -> Option<Box<dyn FnOnce() + Send>> {
        if !state.open && state.active_children.is_empty() && state.pending_resource_scopes == 0 {
            state.settlement_callback.take()
        } else {
            None
        }
    }

    pub(crate) async fn wait_settlement_ready(&self) {
        loop {
            if self.settlement_ready() {
                return;
            }
            let changed = self.children_changed.notified();
            if self.settlement_ready() {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn wait_settlement_ready_blocking(&self) {
        loop {
            if self.settlement_ready() {
                return;
            }
            let generation = self.children_changed.generation();
            if self.settlement_ready() {
                return;
            }
            self.children_changed.wait_blocking_since(generation);
        }
    }

    pub(crate) fn child_input_released(&self, id: OperationId) -> bool {
        {
            let mut state = lock(&self.state);
            let Some(record) = state.active_children.get_mut(&id) else {
                return false;
            };
            record.input_released = true;
        }
        self.settle_child_if_ready(id);
        true
    }

    pub(crate) fn child_input_has_released(&self, id: OperationId) -> bool {
        lock(&self.state)
            .active_children
            .get(&id)
            .is_some_and(|record| record.input_released)
    }

    pub(crate) fn resolve_outcome<T, E>(&self, result: Result<T, E>) -> OperationOutcome<T, E> {
        if let Some(failure) = self.required_failure() {
            return OperationOutcome::ChildFailed(failure);
        }
        if let Some(reason) = self.context.stop_reason() {
            return OperationOutcome::Stopped(StopReport {
                reason,
                accepted_at: None,
            });
        }
        match result {
            Ok(value) => OperationOutcome::Succeeded(value),
            Err(error) => OperationOutcome::Failed(error),
        }
    }

    pub(crate) fn end(&self) {
        let resources = {
            let mut state = lock(&self.state);
            if !state.open {
                return;
            }
            state.open = false;
            std::mem::take(&mut state.resources)
        };
        for resource in resources {
            resource.end();
        }
        self.children_changed.notify_waiters();
        let callback = {
            let mut state = lock(&self.state);
            Self::take_settlement_callback(&mut state)
        };
        invoke_callback(callback);
    }

    #[cfg(test)]
    fn _test_child_count(&self) -> usize {
        lock(&self.state).children.len()
    }
}

fn invoke_callback(callback: Option<Box<dyn FnOnce() + Send + 'static>>) {
    if let Some(callback) = callback {
        callback();
    }
}

/// Non-cloneable authority scoped to one operation body.
pub struct OperationScope {
    pub(crate) core: Arc<ScopeCore>,
    manager: Arc<ManagerCore>,
}
impl OperationScope {
    pub(crate) fn new(core: Arc<ScopeCore>, manager: Arc<ManagerCore>) -> Self {
        Self { core, manager }
    }

    /// Returns this operation's stable identifier.
    pub fn id(&self) -> OperationId {
        self.core.id
    }

    /// Creates an owned manager view bound to this operation's direct children.
    pub fn manager(&self) -> OperationManager {
        let manager = self
            .core
            .manager
            .upgrade()
            .unwrap_or_else(|| Arc::clone(&self.manager));
        OperationManager::bound(manager, &self.core)
    }

    /// Requests stop for this scope and propagates it to descendants.
    pub fn request_stop(&self, reason: StopReason) -> bool {
        self.core.request_stop(reason)
    }

    pub(crate) fn resolve_outcome<T, E>(&self, result: Result<T, E>) -> OperationOutcome<T, E> {
        self.core.resolve_outcome(result)
    }

    /// Returns whether this body frame still accepts children.
    pub(crate) fn is_open(&self) -> bool {
        self.core.is_open()
    }

    #[cfg(test)]
    pub(crate) fn record_test_value(&self, value: usize) {
        self.manager.record_test_value(value);
    }
}

/// Ends a scope when its operation body returns or unwinds, even if moved out.
pub(crate) struct ExecutionFrameGuard {
    scope: Arc<ScopeCore>,
}
impl ExecutionFrameGuard {
    pub(crate) fn new(scope: Arc<ScopeCore>) -> Self {
        Self { scope }
    }
}
impl Drop for ExecutionFrameGuard {
    fn drop(&mut self) {
        self.scope.end();
    }
}

impl ScopeResourceOwner for crate::v02::resource::ResourceScope {
    fn end(&self) {
        crate::v02::resource::ResourceScope::end(self);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use crate::v02::manager::core::ManagerCore;
    use std::sync::Arc;
    use std::sync::Mutex;

    #[test]
    fn scope_manager_is_owned_and_frame_bound() {
        let escaped = Arc::new(Mutex::new(None));
        let saved = Arc::clone(&escaped);
        let (_, manager) = ManagerCore::for_test();
        manager
            .try_submit_body(move |scope| {
                *saved.lock().unwrap() = Some(scope.manager());
            })
            .unwrap();
        let manager = escaped.lock().unwrap().take().unwrap();
        assert!(manager.try_submit_body(|_| {}).is_err());
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::{ScopeCore, ScopeResourceOwner};
    use crate::v02::context::OperationContext;
    use crate::v02::manager::capacity::{ManagerCapacityPools, ScopeCapacity};
    use crate::v02::manager::core::ManagerCore;
    use crate::v02::report::{
        ChildOutcomeKind, ChildOutcomeSummary, DiagnosticSummary, OperationId, OperationOptions,
        SettlementReport, SettlementStatus, StopReason,
    };
    use crate::v02::report::{
        ContextOptions, ManagerId, ManagerLimits, ResourceScopeId, ScopeLimits,
    };
    use crate::v02::resource::cleanup_port::{
        CleanupDispatch, CleanupDispatchPermit, CleanupQueueError,
    };
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct NoCleanup;
    impl CleanupDispatch for NoCleanup {
        fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
            Err(CleanupQueueError::Closed)
        }
    }

    struct EndProbe(Arc<AtomicUsize>);

    impl ScopeResourceOwner for EndProbe {
        fn end(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn outcome(id: OperationId, kind: ChildOutcomeKind) -> ChildOutcomeSummary {
        ChildOutcomeSummary {
            operation_id: id,
            kind,
            summary: DiagnosticSummary::new("child outcome"),
        }
    }

    fn settlement(id: OperationId) -> SettlementReport {
        SettlementReport {
            operation_id: id,
            status: SettlementStatus::Released,
            released_count: 0,
            transferred_count: 0,
            recovery_count: 0,
            children: Arc::from([]),
            children_total: 0,
            children_truncated: false,
            cleanup_failures: Arc::from([]),
            cleanup_failures_total: 0,
            cleanup_failures_truncated: false,
            resource_reports: Arc::from([]),
            resource_reports_total: 0,
            resource_reports_truncated: false,
            transfers: Arc::from([]),
            transfers_total: 0,
            transfers_truncated: false,
            recoveries: Arc::from([]),
            recoveries_total: 0,
            recoveries_truncated: false,
            effects: Arc::from([]),
            effects_total: 0,
            effect_overflow: 0,
        }
    }

    #[test]
    fn scope_end_dispatches_resources_while_child_settlement_is_pending() {
        let ended = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&ended);
        let saved = Arc::new(std::sync::Mutex::new(None::<Arc<ScopeCore>>));
        let saved_in_body = Arc::clone(&saved);
        let (_, manager) = ManagerCore::for_test();
        manager
            .try_submit_body(move |scope| {
                let core = Arc::clone(&scope.core);
                assert!(
                    core.register_resource_owner(Box::new(EndProbe(counter)))
                        .is_ok()
                );
                scope.manager().try_submit_body(|_| {}).unwrap();
                *saved_in_body.lock().unwrap() = Some(core);
            })
            .unwrap();

        let core = saved.lock().unwrap().take().unwrap();
        assert_eq!(ended.load(Ordering::SeqCst), 1);
        assert!(!core.children_settled());
    }

    #[test]
    fn resource_scope_completion_unblocks_parent_settlement() {
        let manager_id = ManagerId::new_unique();
        let manager = ManagerCapacityPools::new(ManagerLimits::default());
        let capacity = ScopeCapacity::root(&manager, 1, ScopeLimits::default());
        let resources = crate::v02::resource::ResourceScope::new(
            manager_id,
            ResourceScopeId::new_unique(),
            Arc::new(capacity),
            Arc::new(NoCleanup),
        );
        let context = OperationContext::new(ContextOptions::default()).unwrap();
        let scope = ScopeCore::new(
            OperationId::new_unique(),
            Arc::downgrade(&ManagerCore::for_test().0),
            None,
            None,
            context,
            OperationOptions::default(),
            8,
        );
        assert!(scope.attach_resource_scope(resources).is_ok());
        assert!(!scope.settlement_ready());
        scope.end();
        assert!(scope.settlement_ready());
        assert!(super::lock(&scope.state).resource_reports.is_empty());
    }

    #[test]
    fn required_failure_is_retained_separately_from_child_settlement() {
        let saved = Arc::new(std::sync::Mutex::new(None::<(Arc<ScopeCore>, OperationId)>));
        let saved_in_body = Arc::clone(&saved);
        let (_, manager) = ManagerCore::for_test();
        manager
            .try_submit_body(move |scope| {
                let parent = Arc::clone(&scope.core);
                let child_id = scope.manager().try_submit_body(|_| {}).unwrap();
                parent
                    .record_child_outcome(outcome(child_id, ChildOutcomeKind::Failed))
                    .unwrap();
                *saved_in_body.lock().unwrap() = Some((parent, child_id));
            })
            .unwrap();

        let (parent, child_id) = saved.lock().unwrap().take().unwrap();
        assert_eq!(parent.required_failure().unwrap().operation_id, child_id);
        assert!(!parent.children_settled());
        assert!(!parent.settlement_ready());
    }
    #[test]
    fn child_outcome_and_settlement_arrive_independently_with_bounded_history() {
        let saved = Arc::new(std::sync::Mutex::new(None::<Arc<ScopeCore>>));
        let saved_in_body = Arc::clone(&saved);
        let (_, manager) = ManagerCore::for_test_with_diagnostic_limit(1);
        manager
            .try_submit_body(move |scope| {
                let parent = Arc::clone(&scope.core);
                for _ in 0..2 {
                    let id = scope.manager().try_submit_body(|_| {}).unwrap();
                    assert!(!parent.record_child_settlement(id, Arc::new(settlement(id)),));
                    assert!(
                        parent
                            .record_child_outcome(outcome(id, ChildOutcomeKind::Succeeded,))
                            .is_ok()
                    );
                    assert!(parent.child_input_released(id));
                }
                *saved_in_body.lock().unwrap() = Some(parent);
            })
            .unwrap();

        let parent = saved.lock().unwrap().take().unwrap();
        assert!(parent.children_settled());
        let (children, total, truncated) = parent.child_reports();
        assert_eq!(children.len(), 1);
        assert_eq!(total, 2);
        assert!(truncated);
        assert!(parent.settlement_ready());
    }

    #[test]
    fn required_failure_stops_siblings_but_optional_failure_does_not_fail_parent() {
        let sibling = Arc::new(std::sync::Mutex::new(None::<Arc<ScopeCore>>));
        let sibling_in_body = Arc::clone(&sibling);
        let (_, manager) = ManagerCore::for_test();
        manager
            .try_submit_body(move |scope| {
                let parent = Arc::clone(&scope.core);
                let failed = scope.manager().try_submit_body(|_| {}).unwrap();
                scope
                    .manager()
                    .try_submit_body(move |sibling_scope| {
                        *sibling_in_body.lock().unwrap() = Some(Arc::clone(&sibling_scope.core));
                    })
                    .unwrap();
                parent
                    .record_child_outcome(outcome(failed, ChildOutcomeKind::Failed))
                    .unwrap();
            })
            .unwrap();
        let sibling = sibling.lock().unwrap().take().unwrap();
        assert_eq!(
            sibling.context.stop_reason(),
            Some(StopReason::RequiredChildFailed)
        );
        assert!(!sibling.mark_child(
            OperationId::new_unique(),
            std::sync::Weak::new(),
            crate::v02::report::ChildRequirement::Required,
        ));

        let parent = Arc::new(std::sync::Mutex::new(None::<Arc<ScopeCore>>));
        let parent_in_body = Arc::clone(&parent);
        let (_, manager) = ManagerCore::for_test();
        manager
            .try_submit_body(move |scope| {
                let core = Arc::clone(&scope.core);
                let mut options = OperationOptions::default();
                options.requirement = crate::v02::report::ChildRequirement::Optional;
                let id = scope
                    .manager()
                    .try_submit_body_with(options, |_| {})
                    .unwrap();
                core.record_child_outcome(outcome(id, ChildOutcomeKind::Failed))
                    .unwrap();
                *parent_in_body.lock().unwrap() = Some(core);
            })
            .unwrap();
        assert!(
            parent
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .required_failure()
                .is_none()
        );
    }

    #[test]
    fn child_release_does_not_substitute_for_its_settlement() {
        let saved = Arc::new(std::sync::Mutex::new(None::<Arc<ScopeCore>>));
        let saved_in_body = Arc::clone(&saved);
        let (_, manager) = ManagerCore::for_test();
        manager
            .try_submit_body(move |scope| {
                let parent = Arc::clone(&scope.core);
                let child_id = scope.manager().try_submit_body(|_| {}).unwrap();
                assert!(parent.child_input_released(child_id));
                assert!(parent.child_input_has_released(child_id));
                assert!(!parent.children_settled());
                *saved_in_body.lock().unwrap() = Some(parent);
            })
            .unwrap();
        let parent = saved.lock().unwrap().take().unwrap();
        assert!(!parent.settlement_ready());
    }

    #[test]
    fn sync_unwind_still_ends_the_scope_frame() {
        let saved = Arc::new(std::sync::Mutex::new(None::<Arc<ScopeCore>>));
        let saved_in_body = Arc::clone(&saved);
        let (_, manager) = ManagerCore::for_test();
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = manager.try_submit_body(move |scope| {
                *saved_in_body.lock().unwrap() = Some(Arc::clone(&scope.core));
                panic!("body panic");
            });
        }));
        assert!(unwind.is_err());
        assert!(!saved.lock().unwrap().as_ref().unwrap().is_open());
    }

    #[test]
    fn required_child_failure_cannot_be_mapped_to_parent_success() {
        let (_, manager) = ManagerCore::for_test();
        manager
            .try_submit_body(move |scope| {
                let child_id = scope.manager().try_submit_body(|_| {}).unwrap();
                scope
                    .core
                    .record_child_outcome(outcome(child_id, ChildOutcomeKind::Failed))
                    .unwrap();
                assert!(matches!(
                    scope.resolve_outcome(Ok::<_, &'static str>("body succeeded")),
                    crate::v02::report::OperationOutcome::ChildFailed(_)
                ));
            })
            .unwrap();
    }
}

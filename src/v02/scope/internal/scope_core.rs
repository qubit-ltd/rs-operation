// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::future::poll_fn;
use std::mem::take;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Poll;

use super::ScopeCoreInit;
#[cfg(test)]
use super::ScopeResourceOwner;
use super::child_record::ChildRecord;
use super::lock;
use super::scope_state::ScopeState;
use super::start_state::StartState;
use crate::v02::adapter::AdapterErrorKind;
use crate::v02::context::OperationContext;
use crate::v02::internal::notify::Notify;
use crate::v02::manager::capacity::PermitBundle;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::manager::core::ManagerCore;
use crate::v02::manager::group::GroupCore;
use crate::v02::operation::OperationCore;
use crate::v02::report::AdmissionError;
use crate::v02::report::ChildFailurePolicy;
use crate::v02::report::ChildFailureSummary;
use crate::v02::report::ChildOutcomeKind;
use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::ChildRequirement;
use crate::v02::report::ChildSettlement;
use crate::v02::report::CleanupFailure;
use crate::v02::report::OperationId;
use crate::v02::report::OperationOptions;
use crate::v02::report::OperationOutcome;
use crate::v02::report::ResourceCleanupReport;
use crate::v02::report::ResourceManageErrorKind;
use crate::v02::report::ResourceProgress;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ResourceScopeReport;
use crate::v02::report::SettlementReport;
use crate::v02::report::StopReason;
use crate::v02::report::StopReport;
use crate::v02::report::TransferReceipt;
use crate::v02::resource::ResourceScope;
use crate::v02::resource::recovery::RecoveryHandle;

/// Resource, cleanup, recovery, and transfer reports with bounded-retention
/// metadata.
type ResourceReportSnapshot = (
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
);

/// Internal scope state referenced weakly by child-bound manager views.
pub(crate) struct ScopeCore {
    /// Stable identity used to correlate the operation and its reports.
    id: OperationId,
    /// Manager retained weakly to avoid a manager-to-scope ownership cycle.
    manager: Weak<ManagerCore>,
    /// Parent scope, if this scope was submitted as a child.
    parent: Option<Weak<ScopeCore>>,
    /// Cancellation, clock, deadline, and budget inherited by this scope.
    context: OperationContext,
    /// Child requirement, capacity and sibling-failure policy for this scope.
    options: OperationOptions,
    /// Maximum number of diagnostics retained in settlement snapshots.
    diagnostic_limit: usize,
    /// Hierarchical permits limiting descendants and resource records.
    capacity: Arc<ScopeCapacity>,
    /// Operation resource registry ended with this scope.
    resource_scope: ResourceScope,
    /// Optional operation group tracking this scope's execution and resources.
    group: Option<Arc<GroupCore>>,
    /// Wakes admission/settlement observers after child-state changes.
    children_changed: Notify,
    /// Child, resource, stop, and completion state guarded as one snapshot.
    state: Mutex<ScopeState>,
    /// Progress aggregated from attached resource scopes.
    resource_progress: Mutex<HashMap<ResourceScopeId, ResourceProgress>>,
    /// Weak link used to publish aggregate resource progress to the operation.
    operation_core: Mutex<Option<Weak<OperationCore>>>,
}
impl ScopeCore {
    /// Creates an open scope with inherited context, limits, and resource
    /// ownership.
    ///
    /// # Parameters
    ///
    /// * `init` — lifecycle state, inherited policies, capacity, resource
    ///   registry, and optional operation group for the new scope.
    ///
    /// # Returns
    ///
    /// A shared scope core connected to the resource scope's lifecycle.
    #[must_use = "the scope core must be retained for the operation lifetime"]
    pub(crate) fn new(init: ScopeCoreInit) -> Arc<Self> {
        let ScopeCoreInit {
            id,
            manager,
            parent,
            context,
            options,
            diagnostic_limit,
            capacity,
            resource_scope,
            group,
        } = init;
        let core = Arc::new(Self {
            id,
            manager,
            parent,
            context,
            options,
            diagnostic_limit,
            capacity,
            resource_scope: resource_scope.internal_clone(),
            group,
            children_changed: Notify::new(),
            resource_progress: Mutex::new(HashMap::new()),
            operation_core: Mutex::new(None),
            state: Mutex::new(ScopeState {
                open: true,
                stopping: false,
                start: StartState::Reserved,
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
        });
        let _ = core.attach_resource_scope(resource_scope);
        core
    }

    /// Borrows the weak manager link used by this scope's bound manager view.
    ///
    /// # Returns
    ///
    /// The weak reference used to access the owning manager while it is alive.
    #[must_use = "use the manager link to access the owning manager while it is alive"]
    #[inline]
    pub(crate) fn manager(&self) -> &Weak<ManagerCore> {
        &self.manager
    }

    /// Returns the stable identity used by operation and child reports.
    ///
    /// # Returns
    ///
    /// This scope's operation identifier.
    #[must_use]
    #[inline]
    pub(crate) fn id(&self) -> OperationId {
        self.id
    }

    /// Reports whether new children or resources can still be attached.
    ///
    /// # Returns
    ///
    /// `true` while this scope accepts new children or resource scopes.
    #[must_use]
    #[inline]
    pub(crate) fn is_open(&self) -> bool {
        lock(&self.state).open
    }

    /// Borrows the inherited cancellation, clock, and budget context.
    ///
    /// # Returns
    ///
    /// The context inherited when this scope was created.
    #[must_use]
    #[inline]
    pub(crate) fn context(&self) -> &OperationContext {
        &self.context
    }

    /// Upgrades the weak parent link while the parent remains alive.
    ///
    /// # Returns
    ///
    /// `Some` with the parent scope while it is alive, or `None` if this is a
    /// root scope or the parent has been dropped.
    #[inline]
    pub(crate) fn parent(&self) -> Option<Arc<ScopeCore>> {
        self.parent.as_ref().and_then(Weak::upgrade)
    }

    /// Borrows the policies and limits used to create direct children.
    ///
    /// # Returns
    ///
    /// The operation options governing this scope's direct children.
    #[must_use]
    #[inline]
    pub(crate) fn options(&self) -> &OperationOptions {
        &self.options
    }

    /// Borrows the hierarchical capacity path owned by this scope.
    ///
    /// # Returns
    ///
    /// The shared capacity path used to admit descendants and resources.
    #[must_use = "use the capacity path to admit descendants and resource scopes"]
    #[inline]
    pub(crate) fn capacity(&self) -> &Arc<ScopeCapacity> {
        &self.capacity
    }

    /// Counts cleanup jobs dispatched for this operation's resource scope.
    ///
    /// # Returns
    ///
    /// The number of cleanup jobs currently in flight.
    #[must_use]
    #[inline]
    pub(crate) fn cleanup_in_flight(&self) -> usize {
        self.resource_scope.cleanup_in_flight()
    }

    /// Borrows the optional operation group tracking this scope.
    ///
    /// # Returns
    ///
    /// `Some` with the group when this scope belongs to one, or `None`
    /// otherwise.
    #[inline]
    pub(crate) fn group(&self) -> Option<&Arc<GroupCore>> {
        self.group.as_ref()
    }

    /// Borrows the resource registry whose end and settlement belong to this
    /// scope.
    ///
    /// # Returns
    ///
    /// The resource registry attached to this operation scope.
    #[must_use = "use the resource scope to manage resources or observe its settlement"]
    #[inline]
    pub(crate) fn resource_scope(&self) -> &ResourceScope {
        &self.resource_scope
    }

    /// Clones the first required child failure that determines this scope's
    /// result.
    ///
    /// # Returns
    ///
    /// `Some` with the first required child failure, or `None` if none has been
    /// recorded.
    pub(crate) fn required_failure(&self) -> Option<ChildFailureSummary> {
        lock(&self.state).required_failure.clone()
    }

    /// Returns the retained child reports and their total/truncation metadata.
    ///
    /// # Returns
    ///
    /// The retained child settlements, total observed child count, and whether
    /// the retained details were truncated.
    #[must_use]
    pub(crate) fn child_reports(&self) -> (Vec<ChildSettlement>, usize, bool) {
        let state = lock(&self.state);
        (state.children.clone(), state.children_total, state.children_truncated)
    }

    /// Clones bounded resource, cleanup, recovery, and transfer report sets.
    ///
    /// # Returns
    ///
    /// The retained resources, cleanup failures, recovery handles, and transfer
    /// receipts. Each collection is followed by its total observed count and a
    /// truncation flag.
    #[must_use]
    pub(crate) fn resource_reports(&self) -> ResourceReportSnapshot {
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

    /// Reports whether every child has been retired after all three lifecycle
    /// facts.
    ///
    /// # Returns
    ///
    /// `true` when no active child remains.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn children_settled(&self) -> bool {
        lock(&self.state).active_children.is_empty()
    }

    /// Checks whether scope end, child settlement, and resource settlement are
    /// complete.
    ///
    /// # Returns
    ///
    /// `true` when the scope is closed and all child and resource scopes have
    /// settled.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn settlement_ready(&self) -> bool {
        let state = lock(&self.state);
        !state.open && state.active_children.is_empty() && state.pending_resource_scopes == 0
    }

    /// Reports whether the child input-release fact has already been recorded.
    ///
    /// # Parameters
    ///
    /// - `self`: Parent scope tracking child lifecycle facts.
    /// - `id`: Child operation whose input-release fact is queried.
    ///
    /// # Returns
    ///
    /// `true` if the active child record confirms input release; otherwise
    /// `false`.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn child_input_has_released(&self, id: OperationId) -> bool {
        lock(&self.state)
            .active_children
            .get(&id)
            .is_some_and(|record| record.input_released)
    }

    /// Binds an operation core and publishes resource progress accumulated
    /// earlier.
    ///
    /// # Parameters
    ///
    /// * `operation` — the operation core receiving aggregate resource
    ///   progress.
    pub(crate) fn bind_operation_core(&self, operation: &Arc<OperationCore>) {
        *lock(&self.operation_core) = Some(Arc::downgrade(operation));
        let progress = lock(&self.resource_progress)
            .values()
            .copied()
            .max_by_key(|progress| resource_progress_rank(*progress));
        operation.set_resource_progress(progress);
    }

    /// Registers a child before it starts, returning false when this scope is
    /// closed.
    ///
    /// # Parameters
    ///
    /// * `id` — identity assigned to the child operation.
    /// * `scope` — weak reference to the child's scope state.
    /// * `requirement` — whether the child is required for parent success.
    ///
    /// # Returns
    ///
    /// `true` if the child was registered, or `false` if this scope is closed,
    /// stopping, or its inherited context has stopped.
    #[must_use = "check whether the child was registered before proceeding"]
    pub(crate) fn mark_child(&self, id: OperationId, scope: Weak<ScopeCore>, requirement: ChildRequirement) -> bool {
        let mut state = lock(&self.state);
        if !state.open || state.stopping || self.context.check().is_err() {
            return false;
        }
        let (children_total, overflowed) = state.children_total.overflowing_add(1);
        state.children_total = if overflowed { usize::MAX } else { children_total };
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

    /// Retains an owner that receives the scope-end event before settlement.
    ///
    /// # Parameters
    ///
    /// * `owner` — resource owner to notify when this scope ends.
    #[cfg(test)]
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

    /// Attaches a resource scope and makes its settlement part of this scope.
    ///
    /// # Parameters
    ///
    /// * `resource_scope` — resource scope whose settlement is tracked here.
    ///
    /// # Errors
    ///
    /// Returns the original resource scope if this operation scope has already
    /// ended.
    pub(crate) fn attach_resource_scope(self: &Arc<Self>, resource_scope: ResourceScope) -> Result<(), ResourceScope> {
        self.attach_resource_scope_inner(resource_scope, None)
    }

    /// Attaches a resource scope while retaining its instance-capacity permit.
    ///
    /// # Parameters
    ///
    /// * `resource_scope` — resource scope whose settlement is tracked here.
    /// * `permit` — capacity permit returned when the resource scope settles.
    ///
    /// # Errors
    ///
    /// Returns the original resource scope if this operation scope has already
    /// ended. The unretained permit is released when the call returns.
    pub(crate) fn attach_resource_scope_with_permit(
        self: &Arc<Self>,
        resource_scope: ResourceScope,
        permit: PermitBundle,
    ) -> Result<(), ResourceScope> {
        self.attach_resource_scope_inner(resource_scope, Some(permit))
    }

    /// Records the first child result, applies required-failure policy, and
    /// wakes outcomes.
    ///
    /// # Parameters
    ///
    /// * `outcome` — outcome reported by a registered child.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the child outcome was recorded. Returns `Err(outcome)` if
    /// the child is unknown, already retired, or already has an outcome.
    pub(crate) fn record_child_outcome(&self, outcome: ChildOutcomeSummary) -> Result<(), ChildOutcomeSummary> {
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

    /// Marks the body frame complete and triggers the callback when children
    /// allow it.
    pub(crate) fn mark_body_finished(&self) {
        let callback = {
            let mut state = lock(&self.state);
            state.body_finished = true;
            Self::take_outcome_callback(&mut state)
        };
        invoke_callback(callback);
    }

    /// Registers the single operation-outcome callback or runs it when already
    /// ready.
    ///
    /// # Parameters
    ///
    /// * `callback` — callback invoked when the required outcome facts are
    ///   ready.
    ///
    /// # Panics
    ///
    /// Panics if an outcome callback has already been registered.
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

    /// Records child settlement separately from its outcome and input release.
    ///
    /// # Parameters
    ///
    /// * `id` — identity of the child operation.
    /// * `settlement` — settlement report produced by the child.
    ///
    /// # Returns
    ///
    /// `true` if recording this settlement retired the child; `false` if the
    /// child is unknown, already has a settlement, or still awaits another
    /// lifecycle fact.
    pub(crate) fn record_child_settlement(&self, id: OperationId, settlement: Arc<SettlementReport>) -> bool {
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

    /// Accepts one stop request and propagates it to descendants.
    ///
    /// # Parameters
    ///
    /// * `reason` — reason recorded for this stop request.
    ///
    /// # Returns
    ///
    /// Whether the inherited context accepted the stop request.
    pub(crate) fn request_stop(&self, reason: StopReason) -> bool {
        let (accepted, descendants) = self.with_lifecycle_gate(|| {
            let mut state = lock(&self.state);
            if state.stopping {
                return (false, Vec::new());
            }
            state.stopping = true;
            if state.start == StartState::Reserved {
                state.start = StartState::Abandoned;
            }
            let descendants = state
                .active_children
                .values()
                .filter_map(|record| record.scope.upgrade())
                .collect::<Vec<_>>();
            (true, descendants)
        });
        if !accepted {
            return false;
        }
        let accepted = self.context.request_stop(reason);
        self.children_changed.notify_waiters();
        for child in descendants {
            child.request_stop(StopReason::ParentStopped);
        }
        accepted
    }

    /// Serializes a resource mutation against scope end, ancestor stops, and
    /// manager shutdown.
    ///
    /// # Type Parameters
    ///
    /// * `R` — result returned by the resource mutation.
    ///
    /// # Parameters
    ///
    /// * `action` — mutation to run while lifecycle changes are excluded.
    ///
    /// # Returns
    ///
    /// The action result if this scope and its ancestors remain active.
    ///
    /// # Errors
    ///
    /// Returns `ScopeEnded` if this scope is closed, or `Stopped` if an
    /// ancestor stop or manager shutdown prevents the mutation.
    pub(crate) fn with_resource_lifecycle<R>(&self, action: impl FnOnce() -> R) -> Result<R, ResourceManageErrorKind> {
        self.with_lifecycle_gate(|| {
            if !self.is_open() {
                return Err(ResourceManageErrorKind::ScopeEnded);
            }
            if self.ancestor_stop_accepted()
                || self
                    .manager
                    .upgrade()
                    .is_some_and(|manager| !manager.adapter_start_allowed())
            {
                return Err(ResourceManageErrorKind::Stopped);
            }
            Ok(action())
        })
    }

    /// Commits a reserved manager child while serialized with every scope stop.
    ///
    /// # Errors
    ///
    /// Returns `Stopped` if an ancestor stop or manager shutdown won the gate,
    /// `ScopeEnded` if this scope is closed, or `AlreadyCommitted` if the start
    /// is not reserved.
    pub(crate) fn commit_start(&self) -> Result<(), AdapterErrorKind> {
        self.with_lifecycle_gate(|| {
            if self.ancestor_stop_accepted() || self.manager_stopped_commits() {
                lock(&self.state).start = StartState::Abandoned;
                return Err(AdapterErrorKind::Stopped);
            }
            let mut state = lock(&self.state);
            if !state.open {
                state.start = StartState::Abandoned;
                return Err(AdapterErrorKind::ScopeEnded);
            }
            if state.start != StartState::Reserved {
                return Err(AdapterErrorKind::AlreadyCommitted);
            }
            state.start = StartState::Committed;
            Ok(())
        })
    }

    /// Activates a committed child unless a stop won the shared lifecycle gate.
    ///
    /// # Errors
    ///
    /// Returns `Stopped` if an ancestor stop or manager shutdown won the gate,
    /// `ScopeEnded` if this scope is closed, or `AlreadyCommitted` if the start
    /// is not committed.
    pub(crate) fn activate_start(&self) -> Result<(), AdapterErrorKind> {
        self.with_lifecycle_gate(|| {
            if self.ancestor_stop_accepted() || self.manager_stopped_commits() {
                lock(&self.state).start = StartState::Abandoned;
                return Err(AdapterErrorKind::Stopped);
            }
            let mut state = lock(&self.state);
            if !state.open {
                state.start = StartState::Abandoned;
                return Err(AdapterErrorKind::ScopeEnded);
            }
            if state.start != StartState::Committed {
                return Err(AdapterErrorKind::AlreadyCommitted);
            }
            state.start = StartState::Activated;
            Ok(())
        })
    }

    /// Marks a reserved or committed start as abandoned during rollback.
    pub(crate) fn abandon_start(&self) {
        self.with_lifecycle_gate(|| {
            let mut state = lock(&self.state);
            if matches!(state.start, StartState::Reserved | StartState::Committed) {
                state.start = StartState::Abandoned;
            }
        });
    }

    /// Awaits a parent becoming unavailable, returning the reason admission can
    /// no longer retry.
    ///
    /// # Returns
    ///
    /// The reason this scope no longer accepts admission attempts.
    pub(crate) async fn wait_unavailable(&self) -> AdmissionError {
        let mut stopped = Box::pin(self.context.stop_requested());
        loop {
            if !self.is_open() {
                return AdmissionError::ScopeEnded;
            }
            if self.context.check().is_err() {
                return AdmissionError::Stopped;
            }
            let mut changed = Box::pin(self.children_changed.notified());
            let result = poll_fn(|cx| {
                if !self.is_open() {
                    return Poll::Ready(Some(AdmissionError::ScopeEnded));
                }
                if self.context.check().is_err() {
                    return Poll::Ready(Some(AdmissionError::Stopped));
                }
                if changed.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(None);
                }
                if stopped.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Some(AdmissionError::Stopped));
                }
                Poll::Pending
            })
            .await;
            if let Some(error) = result {
                return error;
            }
        }
    }

    /// Registers the single settlement callback or invokes it when settlement
    /// is ready.
    ///
    /// # Parameters
    ///
    /// * `callback` — callback invoked when scope and child/resource settlement
    ///   completes.
    ///
    /// # Panics
    ///
    /// Panics if a settlement callback has already been registered.
    pub(crate) fn on_settlement_ready(&self, callback: impl FnOnce() + Send + 'static) {
        let callback = {
            let mut state = lock(&self.state);
            assert!(state.settlement_callback.is_none());
            if !state.open && state.active_children.is_empty() && state.pending_resource_scopes == 0 {
                Some(Box::new(callback) as Box<dyn FnOnce() + Send>)
            } else {
                state.settlement_callback = Some(Box::new(callback));
                None
            }
        };
        invoke_callback(callback);
    }

    /// Records that the child no longer uses parent-provided input.
    ///
    /// # Parameters
    ///
    /// - `self`: Parent scope tracking child lifecycle facts.
    /// - `id`: Child operation whose input-release fact is recorded.
    ///
    /// # Returns
    ///
    /// `true` if the child is still active and its input release was recorded;
    /// otherwise `false` if the child is unknown or already retired.
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

    /// Resolves required-child failure or stop before the body's own result.
    ///
    /// # Type Parameters
    ///
    /// * `T` — the operation body's success value.
    /// * `E` — the operation body's error value.
    ///
    /// # Parameters
    ///
    /// * `result` — the result returned by the operation body.
    ///
    /// # Returns
    ///
    /// A child-failure outcome if a required child failed, a stopped outcome if
    /// the context has a stop reason, or the body's success/failure outcome.
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

    /// Ends child/resource admission, ends registered owners, and signals
    /// settlement.
    pub(crate) fn end(&self) {
        let resources = self.with_lifecycle_gate(|| {
            let mut state = lock(&self.state);
            if !state.open {
                return None;
            }
            state.open = false;
            Some(take(&mut state.resources))
        });
        let Some(resources) = resources else {
            return;
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

    /// Updates aggregate resource progress and forwards it to the operation
    /// handle.
    ///
    /// # Parameters
    ///
    /// * `id` — identity of the resource scope reporting progress.
    /// * `progress` — the reported cleanup progress state.
    fn record_resource_progress(&self, id: ResourceScopeId, progress: ResourceProgress) {
        let progress = {
            let mut values = lock(&self.resource_progress);
            values.insert(id, progress);
            values
                .values()
                .copied()
                .max_by_key(|progress| resource_progress_rank(*progress))
        };
        let operation = lock(&self.operation_core).as_ref().and_then(Weak::upgrade);
        if let Some(operation) = operation {
            operation.set_resource_progress(progress);
        }
    }

    /// Installs progress/settlement callbacks and retains the optional permit.
    ///
    /// # Parameters
    ///
    /// * `resource_scope` — resource scope to observe.
    /// * `permit` — optional permit retained until settlement.
    ///
    /// # Errors
    ///
    /// Returns the original resource scope if this operation scope has already
    /// ended.
    fn attach_resource_scope_inner(
        self: &Arc<Self>,
        resource_scope: ResourceScope,
        permit: Option<PermitBundle>,
    ) -> Result<(), ResourceScope> {
        {
            let mut state = lock(&self.state);
            if !state.open {
                return Err(resource_scope);
            }
            state.pending_resource_scopes = state.pending_resource_scopes.saturating_add(1);
            state.resources.push(Box::new(resource_scope.internal_clone()));
        }
        let weak = Arc::downgrade(self);
        let progress_scope = Arc::downgrade(self);
        let resource_scope_id = resource_scope.id();
        resource_scope.set_progress_callback(Arc::new(move |progress| {
            if let Some(scope) = progress_scope.upgrade() {
                scope.record_resource_progress(resource_scope_id, progress);
            }
        }));
        resource_scope.set_settlement_callback(Box::new(move |report| {
            if let Some(scope) = weak.upgrade() {
                scope.record_resource_scope_report(report);
            }
            drop(permit);
        }));
        Ok(())
    }

    /// Merges bounded cleanup, recovery, and transfer reports from a resource
    /// scope.
    ///
    /// # Parameters
    ///
    /// * `report` — completed resource scope report to aggregate.
    fn record_resource_scope_report(&self, report: ResourceScopeReport) {
        let callback = {
            let mut state = lock(&self.state);
            state.pending_resource_scopes = state.pending_resource_scopes.saturating_sub(1);
            state.resource_reports_total = state.resource_reports_total.saturating_add(report.resources.len());
            for resource in report.resources.iter().cloned() {
                if state.resource_reports.len() < self.diagnostic_limit {
                    state.resource_reports.push(resource);
                } else {
                    state.resource_reports_truncated = true;
                }
            }
            state.cleanup_failures_total = state.cleanup_failures_total.saturating_add(report.failures.len());
            for failure in report.failures.iter().cloned() {
                if state.cleanup_failures.len() < self.diagnostic_limit {
                    state.cleanup_failures.push(failure);
                } else {
                    state.cleanup_failures_truncated = true;
                }
            }
            state.recoveries_total = state.recoveries_total.saturating_add(report.recoveries.len());
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

    /// Checks whether the body ended and all required children produced
    /// outcomes.
    ///
    /// # Parameters
    ///
    /// * `state` — scope state snapshot to inspect.
    ///
    /// # Returns
    ///
    /// `true` when the body has finished and every required child has an
    /// outcome.
    #[must_use]
    #[inline]
    fn outcomes_ready(state: &ScopeState) -> bool {
        state.body_finished
            && state
                .active_children
                .values()
                .all(|record| record.requirement != ChildRequirement::Required || record.outcome.is_some())
    }

    /// Takes the outcome callback exactly once after the required facts are
    /// ready.
    ///
    /// # Parameters
    ///
    /// * `state` — mutable scope state containing the pending callback.
    ///
    /// # Returns
    ///
    /// `Some(callback)` when required outcome facts are ready, otherwise
    /// `None`.
    fn take_outcome_callback(state: &mut ScopeState) -> Option<Box<dyn FnOnce() + Send>> {
        if Self::outcomes_ready(state) {
            state.outcome_callback.take()
        } else {
            None
        }
    }

    /// Retires a child only after outcome, settlement, and input release are
    /// all known.
    ///
    /// # Parameters
    ///
    /// * `id` — identity of the child to retire if all lifecycle facts are
    ///   ready.
    ///
    /// # Returns
    ///
    /// `true` if the child had outcome, settlement, and input-release facts and
    /// was retired; otherwise `false`.
    fn settle_child_if_ready(&self, id: OperationId) -> bool {
        let settled = {
            let mut state = lock(&self.state);
            let ready = state
                .active_children
                .get(&id)
                .is_some_and(|record| record.outcome.is_some() && record.settlement.is_some() && record.input_released);
            if !ready {
                return false;
            }
            let record = state.active_children.remove(&id).expect("ready child record exists");
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

    /// Checks this scope and every ancestor for an accepted stop.
    ///
    /// # Returns
    ///
    /// `true` if this scope or one of its ancestors has accepted a stop.
    #[must_use]
    fn ancestor_stop_accepted(&self) -> bool {
        lock(&self.state).stopping
            || self.context.stop_reason().is_some()
            || self
                .parent
                .as_ref()
                .and_then(Weak::upgrade)
                .is_some_and(|parent| parent.ancestor_stop_accepted())
    }

    /// Runs `action` under the manager-wide start/stop gate when available.
    ///
    /// # Type Parameters
    ///
    /// * `R` — result returned by `action`.
    ///
    /// # Parameters
    ///
    /// * `action` — operation to run under the lifecycle gate.
    ///
    /// # Returns
    ///
    /// The value returned by `action`.
    #[inline]
    fn with_lifecycle_gate<R>(&self, action: impl FnOnce() -> R) -> R {
        match self.manager.upgrade() {
            Some(manager) => manager.with_lifecycle_gate(action),
            None => action(),
        }
    }

    /// Checks whether manager shutdown prevents any further start commit.
    ///
    /// # Returns
    ///
    /// `true` if the manager no longer permits a child start to commit.
    #[must_use]
    #[inline]
    fn manager_stopped_commits(&self) -> bool {
        self.manager
            .upgrade()
            .is_some_and(|manager| !manager.adapter_start_allowed())
    }

    /// Takes the settlement callback once scope, children, and resource scopes
    /// are complete.
    ///
    /// # Parameters
    ///
    /// * `state` — mutable scope state containing the pending callback.
    ///
    /// # Returns
    ///
    /// `Some(callback)` when the scope is closed and all child and resource
    /// scopes have settled; otherwise `None`.
    fn take_settlement_callback(state: &mut ScopeState) -> Option<Box<dyn FnOnce() + Send>> {
        if !state.open && state.active_children.is_empty() && state.pending_resource_scopes == 0 {
            state.settlement_callback.take()
        } else {
            None
        }
    }

    #[cfg(test)]
    fn _test_child_count(&self) -> usize {
        lock(&self.state).children.len()
    }
}

/// Orders progress so aggregate snapshots report the furthest cleanup phase.
///
/// # Parameters
///
/// * `progress` — resource cleanup progress to rank.
///
/// # Returns
///
/// A rank from zero through three, increasing from scope-open to unconfirmed
/// cleanup.
#[must_use]
#[inline]
fn resource_progress_rank(progress: ResourceProgress) -> u8 {
    match progress {
        ResourceProgress::ScopeOpen => 0,
        ResourceProgress::WaitingForUsers => 1,
        ResourceProgress::Cleaning => 2,
        ResourceProgress::Unconfirmed => 3,
    }
}

/// Runs a callback after its state mutex has been released.
///
/// # Parameters
///
/// * `callback` — optional callback to invoke outside the mutex.
#[inline]
fn invoke_callback(callback: Option<Box<dyn FnOnce() + Send + 'static>>) {
    if let Some(callback) = callback {
        callback();
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::Weak;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::ScopeCore;
    use super::ScopeCoreInit;
    use super::ScopeResourceOwner;
    use crate::v02::context::OperationContext;
    use crate::v02::manager::capacity::ManagerCapacityPools;
    use crate::v02::manager::capacity::ScopeCapacity;
    use crate::v02::manager::core::ManagerCore;
    use crate::v02::report::ChildOutcomeKind;
    use crate::v02::report::ChildOutcomeSummary;
    use crate::v02::report::ChildRequirement;
    use crate::v02::report::ContextOptions;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ManagerLimits;
    use crate::v02::report::OperationId;
    use crate::v02::report::OperationOptions;
    use crate::v02::report::OperationOutcome;
    use crate::v02::report::ResourceManageErrorKind;
    use crate::v02::report::ResourceScopeId;
    use crate::v02::report::ScopeLimits;
    use crate::v02::report::SettlementReport;
    use crate::v02::report::SettlementStatus;
    use crate::v02::report::StopReason;
    use crate::v02::resource::ResourceScope;
    use crate::v02::resource::cleanup_port::CleanupDispatch;
    use crate::v02::resource::cleanup_port::CleanupDispatchPermit;
    use crate::v02::resource::cleanup_port::CleanupQueueError;

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
    fn test_scope_end_dispatches_resources_while_child_settlement_is_pending() {
        let ended = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&ended);
        let saved = Arc::new(Mutex::new(None::<Arc<ScopeCore>>));
        let saved_in_body = Arc::clone(&saved);
        let (_, manager) = ManagerCore::for_test();
        manager
            .try_submit_body(move |scope| {
                let core = Arc::clone(&scope.core);
                assert!(core.register_resource_owner(Box::new(EndProbe(counter))).is_ok());
                scope.manager().try_submit_body(|_| {}).unwrap();
                *saved_in_body.lock().unwrap() = Some(core);
            })
            .unwrap();

        let core = saved.lock().unwrap().take().unwrap();
        assert_eq!(ended.load(Ordering::SeqCst), 1);
        assert!(!core.children_settled());
    }

    #[test]
    fn test_resource_scope_completion_unblocks_parent_settlement() {
        let manager_id = ManagerId::new_unique();
        let manager = ManagerCapacityPools::new(ManagerLimits::default());
        let capacity = ScopeCapacity::root(&manager, 1, ScopeLimits::default());
        let capacity = Arc::new(capacity);
        let resources = ResourceScope::new(
            manager_id,
            ResourceScopeId::new_unique(),
            Arc::clone(&capacity),
            Arc::new(NoCleanup),
        );
        let context = OperationContext::new(ContextOptions::default()).unwrap();
        let scope = ScopeCore::new(ScopeCoreInit {
            id: OperationId::new_unique(),
            manager: Arc::downgrade(&ManagerCore::for_test().0),
            parent: None,
            context,
            options: OperationOptions::default(),
            diagnostic_limit: 8,
            capacity,
            resource_scope: resources.internal_clone(),
            group: None,
        });
        assert!(!scope.settlement_ready());
        scope.end();
        assert!(scope.settlement_ready());
        assert!(super::lock(&scope.state).resource_reports.is_empty());
    }

    #[test]
    fn test_attached_resource_scope_holds_and_releases_its_capacity_permit() {
        let (_, manager) = ManagerCore::for_test();
        manager
            .try_submit_body(|operation_scope| {
                let scope = Arc::clone(&operation_scope.core);
                let id = ResourceScopeId::new_unique();
                let capacity = Arc::new(scope.capacity().resource_scope_for(id, 1));
                let resources = ResourceScope::new(ManagerId::new_unique(), id, capacity, Arc::new(NoCleanup));
                let permit = scope.capacity().resource_scope_permits().unwrap();

                assert!(matches!(
                    scope.attach_resource_scope_with_permit(resources.internal_clone(), permit),
                    Ok(())
                ));
                assert_eq!(super::lock(&scope.state).pending_resource_scopes, 2);
                let rejected = resources
                    .manage(7usize)
                    .err()
                    .expect("test cleanup dispatcher rejects resource admission");
                assert_eq!(rejected.kind(), ResourceManageErrorKind::CapacityExceeded);

                resources.end();

                assert_eq!(super::lock(&scope.state).pending_resource_scopes, 1);
            })
            .unwrap();
    }

    #[test]
    fn test_required_failure_is_retained_separately_from_child_settlement() {
        let saved = Arc::new(Mutex::new(None::<(Arc<ScopeCore>, OperationId)>));
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
    fn test_child_outcome_and_settlement_arrive_independently_with_bounded_history() {
        let saved = Arc::new(Mutex::new(None::<Arc<ScopeCore>>));
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
                assert_eq!(parent._test_child_count(), 1);
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
    fn test_required_failure_stops_siblings_but_optional_failure_does_not_fail_parent() {
        let sibling = Arc::new(Mutex::new(None::<Arc<ScopeCore>>));
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
        assert_eq!(sibling.context.stop_reason(), Some(StopReason::RequiredChildFailed));
        assert!(!sibling.mark_child(OperationId::new_unique(), Weak::new(), ChildRequirement::Required,));

        let parent = Arc::new(Mutex::new(None::<Arc<ScopeCore>>));
        let parent_in_body = Arc::clone(&parent);
        let (_, manager) = ManagerCore::for_test();
        manager
            .try_submit_body(move |scope| {
                let core = Arc::clone(&scope.core);
                let options = OperationOptions {
                    requirement: ChildRequirement::Optional,
                    ..OperationOptions::default()
                };
                let id = scope.manager().try_submit_body_with(options, |_| {}).unwrap();
                core.record_child_outcome(outcome(id, ChildOutcomeKind::Failed))
                    .unwrap();
                *parent_in_body.lock().unwrap() = Some(core);
            })
            .unwrap();
        assert!(parent.lock().unwrap().as_ref().unwrap().required_failure().is_none());
    }

    #[test]
    fn test_child_release_does_not_substitute_for_its_settlement() {
        let saved = Arc::new(Mutex::new(None::<Arc<ScopeCore>>));
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
    fn test_sync_unwind_still_ends_the_scope_frame() {
        let saved = Arc::new(Mutex::new(None::<Arc<ScopeCore>>));
        let saved_in_body = Arc::clone(&saved);
        let (_, manager) = ManagerCore::for_test();
        let unwind = catch_unwind(AssertUnwindSafe(|| {
            let _ = manager.try_submit_body(move |scope| {
                *saved_in_body.lock().unwrap() = Some(Arc::clone(&scope.core));
                panic!("body panic");
            });
        }));
        assert!(unwind.is_err());
        assert!(!saved.lock().unwrap().as_ref().unwrap().is_open());
    }

    #[test]
    fn test_required_child_failure_cannot_be_mapped_to_parent_success() {
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
                    OperationOutcome::ChildFailed(_)
                ));
            })
            .unwrap();
    }
}

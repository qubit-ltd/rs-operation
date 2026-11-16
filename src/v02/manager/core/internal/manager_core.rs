// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::Weak;
use std::task::Poll;

use super::super::child_outcome_summary;
use super::super::lock;
use super::super::map_context_error;
use super::ExecutionKind;
use super::ManagerPhase;
use super::OperationJob;
use super::SubmissionLease;
use super::SubmissionPermit;
use super::SubmissionPort;
use super::SubmissionReservation;
use super::SubmissionTarget;
use super::manager_state::ManagerState;
use crate::v02::adapter::AdapterErrorKind;
use crate::v02::adapter::AdapterReservation;
use crate::v02::adapter::ManagedOperationControl;
use crate::v02::context::OperationContext;
use crate::v02::manager::core::OperationManager;
#[cfg(test)]
use crate::v02::manager::core::TestPort;
#[cfg(test)]
use crate::v02::manager::core::dispose_panic_payload;
use crate::v02::operation::OperationCore;
use crate::v02::operation::OutcomeSlot;
use crate::v02::report::AdmissionError;
use crate::v02::report::ChildRelease;
use crate::v02::report::ExecutionPhase;
use crate::v02::report::OperationId;
use crate::v02::report::OperationOptions;
use crate::v02::report::OperationOutcome;
use crate::v02::report::ScopeLimits;
use crate::v02::report::SettlementReport;
use crate::v02::report::SettlementStatus;
use crate::v02::report::StopReason;
use crate::v02::report::StopReport;
use crate::v02::report::SubmitError;
use crate::v02::resource::ResourceScope;
#[cfg(test)]
use crate::v02::scope::ExecutionContextFuture;
#[cfg(test)]
use crate::v02::scope::ExecutionContextGuard;
#[cfg(test)]
use crate::v02::scope::ExecutionFrameGuard;
#[cfg(test)]
use crate::v02::scope::OperationScope;
use crate::v02::scope::ScopeCore;
use crate::v02::scope::ScopeCoreInit;

/// State shared by all sync and async manager views.
pub(crate) struct ManagerCore {
    /// Mutable scope, service-resource, and recovery registry.
    pub(in crate::v02::manager::core) state: Mutex<ManagerState>,
    /// Serializes scope stop with adapter commit/activation transitions.
    lifecycle_gate: Mutex<()>,
    /// Wakes blocking shutdown observers after tracked state changes.
    shutdown_changed: Condvar,
    /// Wakes async shutdown observers after tracked state changes.
    shutdown_notify: crate::v02::internal::notify::Notify,
    /// Supplies reservations and owns the concrete executor/resource assembly.
    port: Arc<dyn SubmissionPort>,
    /// Context inherited by root operations.
    root_context: Arc<OperationContext>,
    /// Default limits inherited by operations using default options.
    scope_limits: ScopeLimits,
    /// Maximum number of diagnostic entries retained in each scope.
    diagnostic_limit: usize,
    /// Maximum number of group policies that may be attached to one operation.
    pub(crate) max_groups_per_operation: usize,
}

impl ManagerCore {
    /// Creates a coordinator with a bounded default diagnostic history.
    ///
    /// # Parameters
    ///
    /// - `port`: reservation port used by test operations.
    /// - `root_context`: context inherited by root operations.
    /// - `scope_limits`: default capacity limits for operation scopes.
    #[cfg(test)]
    pub(crate) fn new(
        port: Arc<dyn SubmissionPort>,
        root_context: OperationContext,
        scope_limits: ScopeLimits,
    ) -> Arc<Self> {
        Self::new_with_diagnostic_limit(port, root_context, scope_limits, 256, 16)
    }

    /// Builds a coordinator and bounds diagnostic retention per operation.
    ///
    /// `diagnostic_limit` limits retained diagnostic entries; it does not limit
    /// operation or resource capacity.
    ///
    /// # Parameters
    ///
    /// - `port`: reservation port that assembles execution and resource
    ///   capacity.
    /// - `root_context`: context inherited by root operations.
    /// - `scope_limits`: default limits used by operation scopes.
    /// - `diagnostic_limit`: maximum diagnostic entries retained per scope.
    /// - `max_groups_per_operation`: maximum group policies attached to an
    ///   operation.
    ///
    /// # Returns
    ///
    /// A shared coordinator initialized in the open phase.
    pub(crate) fn new_with_diagnostic_limit(
        port: Arc<dyn SubmissionPort>,
        root_context: OperationContext,
        scope_limits: ScopeLimits,
        diagnostic_limit: usize,
        max_groups_per_operation: usize,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ManagerState {
                phase: ManagerPhase::Open,
                shutdown_mode: crate::v02::report::ShutdownMode::Drain,
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
            max_groups_per_operation,
        })
    }

    /// Serializes scope stops with adapter commit and activation transitions.
    ///
    /// # Type Parameters
    ///
    /// - `R`: result produced by `action`.
    ///
    /// # Parameters
    ///
    /// - `action`: operation to run while scope lifecycle transitions are
    ///   blocked.
    /// - `self`: coordinator whose lifecycle transitions are serialized.
    ///
    /// # Returns
    ///
    /// The value returned by `action`.
    pub(crate) fn with_lifecycle_gate<R>(&self, action: impl FnOnce() -> R) -> R {
        let _gate = lock(&self.lifecycle_gate);
        action()
    }

    /// Reports whether an already admitted adapter child may start.
    ///
    /// # Returns
    ///
    /// `true` while the manager accepts starts from children already admitted.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator whose phase is checked.
    #[must_use]
    #[inline]
    pub(crate) fn adapter_start_allowed(&self) -> bool {
        matches!(
            lock(&self.state).phase,
            ManagerPhase::Open | ManagerPhase::Draining
        )
    }

    /// Creates the root sync view sharing this coordinator.
    ///
    /// # Returns
    ///
    /// The root manager view backed by this coordinator.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator shared by the returned root view.
    pub(crate) fn root_view(self: &Arc<Self>) -> OperationManager {
        OperationManager::from_parts(
            Arc::clone(self),
            SubmissionTarget::Root { groups: Vec::new() },
        )
    }

    /// Creates a child-targeted view that becomes invalid when `scope` ends.
    ///
    /// # Parameters
    ///
    /// - `scope`: parent scope that bounds the returned manager view.
    /// - `self`: coordinator backing the returned child-targeted view.
    ///
    /// # Returns
    ///
    /// A manager view that submits operations beneath `scope`.
    pub(crate) fn bound(self: Arc<Self>, scope: &Arc<ScopeCore>) -> OperationManager {
        OperationManager::from_parts(
            self,
            SubmissionTarget::Parent {
                id: scope.id(),
                scope: Arc::downgrade(scope),
                groups: scope.groups().to_vec(),
            },
        )
    }
}

impl ManagerCore {
    /// Reserves all resources and registers a child scope before dispatch.
    ///
    /// On failure, returns the unconsumed body with the admission reason.
    ///
    /// # Type Parameters
    ///
    /// - `F`: operation body retained for dispatch or returned on admission
    ///   failure.
    ///
    /// # Parameters
    ///
    /// - `target`: root or parent scope that receives the operation.
    /// - `options`: operation-specific scope and context limits.
    /// - `execution`: execution mode reserved for the operation.
    /// - `body`: operation body to retain until dispatch.
    /// - `self`: coordinator reserving capacity and registering the scope.
    ///
    /// # Returns
    ///
    /// The body, operation ID, scope core, and reservation; on failure, the
    /// body is returned with the admission error.
    ///
    /// # Errors
    ///
    /// Returns the original `body` with the admission failure that prevented
    /// reservation.
    pub(in crate::v02::manager::core) fn reserve_scope<F>(
        self: &Arc<Self>,
        target: &SubmissionTarget,
        options: OperationOptions,
        execution: ExecutionKind,
        body: F,
    ) -> Result<(F, OperationId, Arc<ScopeCore>, SubmissionReservation), SubmitError<F>>
    where
        F: Send + 'static,
    {
        self.reserve_scope_with_preflight(target, options, execution, body, |_| Ok(()), false)
    }

    /// Reserves a scope while running a one-shot identity claim after
    /// capacity/executor admission succeeds and before parent/group
    /// registration.
    ///
    /// # Type Parameters
    ///
    /// - `F`: Operation body retained for dispatch or returned on admission failure.
    /// - `C`: Preflight callback that claims the assigned operation identity.
    ///
    /// # Parameters
    ///
    /// - `self`: Shared coordinator reserving capacity and registering the scope.
    /// - `target`: Root or parent scope that receives the operation.
    /// - `options`: Operation-specific scope and context limits.
    /// - `execution`: Execution mode reserved for the operation.
    /// - `body`: Operation body to retain until dispatch.
    /// - `preflight`: Identity claim invoked after admission succeeds.
    /// - `allow_claimed_after_close`: Whether a previously claimed operation may
    ///   proceed after admission closes.
    ///
    /// # Returns
    ///
    /// The body, operation ID, scope core, and reservation; on failure, the
    /// body is returned with the admission error.
    ///
    /// # Errors
    ///
    /// Returns the original body with the admission failure that prevented
    /// reservation or registration.
    pub(in crate::v02::manager::core) fn reserve_scope_with_preflight<F, C>(
        self: &Arc<Self>,
        target: &SubmissionTarget,
        mut options: OperationOptions,
        execution: ExecutionKind,
        body: F,
        preflight: C,
        allow_claimed_after_close: bool,
    ) -> Result<(F, OperationId, Arc<ScopeCore>, SubmissionReservation), SubmitError<F>>
    where
        F: Send + 'static,
        C: FnOnce(OperationId) -> Result<(), AdmissionError>,
    {
        let (parent, groups) = match target {
            SubmissionTarget::Root { groups } => (None, groups),
            SubmissionTarget::Parent { id, scope, groups } => {
                let Some(scope) = scope.upgrade() else {
                    return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
                };
                if !scope.is_open() {
                    return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
                }
                if let Err(error) = scope.context().check() {
                    return Err(SubmitError::new(map_context_error(error), body));
                }
                (Some((*id, scope)), groups)
            }
        };
        let group_permits =
            match crate::v02::manager::group::GroupCore::reserve_many(groups, parent.is_some()) {
                Ok(permits) => permits,
                Err(reason) => return Err(SubmitError::new(reason, body)),
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
        let scope_core = ScopeCore::new(ScopeCoreInit {
            id,
            manager: Arc::downgrade(self),
            parent: parent.as_ref().map(|(_, scope)| Arc::downgrade(scope)),
            context,
            options,
            diagnostic_limit: self.diagnostic_limit,
            capacity: Arc::clone(&permit.capacity),
            resource_scope: permit.resource_scope.internal_clone(),
            groups: groups.clone(),
        });
        // Reserve the parent relationship before bind, while keeping foreign
        // code outside locks. This lets parent shutdown see pending work.
        let provisional_parent = allow_claimed_after_close
            && parent.as_ref().is_some_and(|(_, parent_scope)| {
                parent_scope.reserve_child(
                    id,
                    Arc::downgrade(&scope_core),
                    scope_core.options().requirement,
                )
            });
        if allow_claimed_after_close && parent.is_some() && !provisional_parent {
            return Err(SubmitError::new(AdmissionError::ScopeEnded, body));
        }
        if let Err(reason) = preflight(id) {
            if provisional_parent {
                parent
                    .as_ref()
                    .expect("parent reservation has a parent")
                    .1
                    .rollback_child_reservation(id);
            }
            return Err(SubmitError::new(reason, body));
        }
        if provisional_parent {
            parent
                .as_ref()
                .expect("parent reservation has a parent")
                .1
                .commit_child_reservation(id);
        }
        self.track_resource_recoveries(&permit.resource_scope, groups.clone());
        {
            let mut state = lock(&self.state);
            let allowed = allow_claimed_after_close
                || match (state.phase, parent_id) {
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
            let register_result =
                crate::v02::manager::group::GroupCore::register_scope_many_with_claimed(
                    groups,
                    id,
                    Arc::downgrade(&scope_core),
                    parent_id.is_some() || allow_claimed_after_close,
                    allow_claimed_after_close,
                    || {
                        if !provisional_parent
                            && let Some((_, parent_scope)) = parent.as_ref()
                            && !parent_scope.mark_child(
                                id,
                                Arc::downgrade(&scope_core),
                                scope_core.options().requirement,
                            )
                        {
                            return Err(AdmissionError::ScopeEnded);
                        }
                        Ok(())
                    },
                );
            if let Err(reason) = register_result {
                drop(state);
                return Err(SubmitError::new(reason, body));
            }
            state.active_scopes.insert(id, Arc::downgrade(&scope_core));
            #[cfg(test)]
            state.relationships.push((id, parent_id));
        }
        let group_cancelled = allow_claimed_after_close
            && groups
                .iter()
                .any(|group| group.mode() == crate::v02::report::ShutdownMode::Cancel);
        if group_cancelled {
            scope_core.request_stop(crate::v02::report::StopReason::ParentStopped);
        }
        let mut permit = permit;
        for group_permit in group_permits {
            permit.lifecycle = Arc::new(crate::v02::manager::group::GroupLease::new(
                Arc::clone(&permit.lifecycle),
                group_permit,
            ));
        }
        Ok((body, id, scope_core, permit))
    }

    #[cfg(test)]
    pub(in crate::v02::manager::core) fn submit_scope<F>(
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
                let manager = scope_core
                    .manager()
                    .upgrade()
                    .expect("active scope manager exists");
                let groups = scope_core.groups().to_vec();
                let _context = ExecutionContextGuard::enter(&manager, id, &groups);
                body(scope);
            })));
        Ok(id)
    }

    #[cfg(test)]
    pub(in crate::v02::manager::core) fn submit_async_scope<F, Fut>(
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
            let manager = scope
                .core
                .manager()
                .upgrade()
                .expect("active scope manager exists");
            let groups = scope.core.groups().to_vec();
            let user_future = {
                let _context = ExecutionContextGuard::enter(&manager, id, &groups);
                body(scope)
            };
            if let Err(payload) =
                ExecutionContextFuture::new(user_future, manager, id, groups).await
            {
                dispose_panic_payload(payload);
            }
        };
        permit
            .execution
            .dispatch(OperationJob::Async(Box::pin(future)));
        Ok(id)
    }
}

impl ManagerCore {
    /// Snapshots bounded child, resource, cleanup, transfer, and recovery
    /// reports.
    ///
    /// # Parameters
    ///
    /// - `scope`: settled or settling scope whose reports are collected.
    /// - `self`: coordinator producing the bounded settlement snapshot.
    ///
    /// # Returns
    ///
    /// A bounded snapshot of the scope's settlement evidence.
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
        let (effects, effects_total, effect_overflow) = scope.effect_records();
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
            effects: Arc::from(effects),
            effects_total,
            effect_overflow,
        }
    }
}

impl ManagerCore {
    /// Publishes settlement once the scope is ready and releases its permits.
    ///
    /// # Parameters
    ///
    /// - `scope`: scope whose completion triggers publication.
    /// - `operation`: operation receiving the settlement report.
    /// - `lease`: reservation lease released after settlement.
    /// - `control`: managed control notified when publication completes.
    /// - `self`: coordinator registering the settlement callback.
    pub(in crate::v02::manager::core) fn register_settlement(
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
            for group in scope_for_callback.groups() {
                group.settlement_finished(operation_id);
            }
            manager.shutdown_changed.notify_all();
            manager.shutdown_notify.notify_waiters();
        });
    }

    /// Attaches, commits, and activates a reserved operation through the
    /// adapter.
    ///
    /// Returns an adapter lifecycle error; `abandon` handles failed dispatch.
    ///
    /// # Parameters
    ///
    /// - `scope`: scope being committed and activated.
    /// - `control`: operation control attached to the adapter.
    /// - `execution`: execution permit used to dispatch the job.
    /// - `job`: operation job transferred to the adapter.
    /// - `abandon`: cleanup callback used when dispatch cannot proceed.
    /// - `self`: coordinator dispatching the reserved adapter job.
    ///
    /// # Returns
    ///
    /// `Ok(())` after activation, or the adapter lifecycle error.
    ///
    /// # Errors
    ///
    /// Returns the adapter lifecycle failure when attach, commit, or activation
    /// cannot proceed.
    pub(in crate::v02::manager::core) fn dispatch_adapter_job(
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

    /// Completes a reservation that could not be activated as a stopped
    /// operation.
    ///
    /// # Type Parameters
    ///
    /// - `T`: successful operation output type.
    /// - `E`: operation error type.
    ///
    /// # Parameters
    ///
    /// - `scope`: scope whose reserved start is abandoned.
    /// - `operation`: operation marked finished by the failed dispatch.
    /// - `slot`: outcome slot receiving the stopped result.
    /// - `lease`: reservation lease whose execution permit is released.
    /// - `parent`: optional parent scope receiving the child outcome.
    /// - `control`: operation control notified about input release.
    pub(in crate::v02::manager::core) fn abandon_reserved_operation<
        T: Send + 'static,
        E: Send + 'static,
    >(
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
}

impl ManagerCore {
    /// Moves the manager toward draining or cancellation and ends service
    /// scopes.
    ///
    /// # Parameters
    ///
    /// - `mode`: shutdown policy requested by the caller.
    /// - `self`: coordinator whose shutdown phase is updated.
    ///
    /// # Returns
    ///
    /// The resulting manager phase.
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
            if state.phase != ManagerPhase::Closed
                && mode == crate::v02::report::ShutdownMode::Cancel
            {
                state.shutdown_mode = crate::v02::report::ShutdownMode::Cancel;
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
        self.shutdown_notify.notify_waiters();
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

    /// Returns the strongest shutdown policy requested for this manager.
    pub(crate) fn shutdown_mode(&self) -> crate::v02::report::ShutdownMode {
        lock(&self.state).shutdown_mode
    }

    /// Reports whether an operation still contributes to the manager shutdown.
    ///
    /// # Parameters
    ///
    /// - `id`: Operation whose active registration is checked.
    ///
    /// # Returns
    ///
    /// `true` while the operation remains registered.
    pub(crate) fn has_active_scope(&self, id: OperationId) -> bool {
        lock(&self.state).active_scopes.contains_key(&id)
    }

    /// Returns the clock source inherited by root operation contexts.
    ///
    /// # Returns
    ///
    /// The configured clock source, or `None` when root contexts have no clock.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator containing the root context.
    #[inline]
    pub(crate) fn clock(&self) -> Option<Arc<dyn crate::v02::protocol::ClockSource>> {
        self.root_context.clock_source()
    }

    /// Counts operation scopes that have not yet settled.
    ///
    /// # Returns
    ///
    /// The number of active operation scopes.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator whose active scopes are counted.
    #[inline]
    pub(crate) fn active_scope_count(&self) -> usize {
        lock(&self.state).active_scopes.len()
    }

    /// Closes the executor after all tracked scopes and service scopes settle.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator whose tracked state is checked before shutdown.
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

    /// Registers a service resource scope whose lifetime is owned by the
    /// manager. Creates a manager-lifetime resource scope and registers it
    /// for shutdown.
    ///
    /// # Parameters
    ///
    /// - `capacity`: maximum number of resources managed by the scope.
    /// - `group`: optional operation group that tracks this service scope.
    /// - `self`: coordinator registering the manager-lifetime resource scope.
    ///
    /// # Returns
    ///
    /// The registered resource scope.
    ///
    /// # Errors
    ///
    /// Returns `ResourceScopeError` when creation fails or shutdown has
    /// started.
    pub(crate) fn service_resource_scope(
        self: &Arc<Self>,
        capacity: usize,
        groups: Vec<Arc<crate::v02::manager::group::GroupCore>>,
    ) -> Result<ResourceScope, crate::v02::report::ResourceScopeError> {
        let scope = self.port.service_resource_scope(capacity)?;
        self.track_resource_recoveries(&scope, groups.clone());
        let id = scope.id();
        let manager = Arc::downgrade(self);
        let group_callbacks = groups.iter().map(Arc::downgrade).collect::<Vec<_>>();
        scope.set_settlement_callback(Box::new(move |_| {
            let removed = if let Some(manager) = manager.upgrade() {
                let removed = lock(&manager.state).service_scopes.remove(&id);
                manager.shutdown_changed.notify_all();
                manager.shutdown_notify.notify_waiters();
                removed
            } else {
                None
            };
            for group in group_callbacks.iter().filter_map(Weak::upgrade) {
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
        let mut registered_groups: Vec<&Arc<crate::v02::manager::group::GroupCore>> = Vec::new();
        for group in &groups {
            if !group.register_service(scope.internal_clone()) {
                for registered in registered_groups {
                    registered.service_finished(id);
                }
                scope.end();
                return Err(crate::v02::report::ResourceScopeError::ScopeEnded);
            }
            registered_groups.push(group);
        }
        Ok(scope)
    }

    /// Creates a resource scope attached to an open operation scope.
    ///
    /// # Parameters
    ///
    /// - `owner`: open operation scope that owns the resource scope.
    /// - `options`: resource capacity and cleanup options.
    /// - `self`: coordinator creating the attached resource scope.
    ///
    /// # Returns
    ///
    /// The attached resource scope.
    ///
    /// # Errors
    ///
    /// Returns `ResourceScopeError` when options are invalid, capacity is
    /// exhausted, creation fails, or the owner has ended.
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
        let scope_permit = owner
            .capacity()
            .resource_scope_permits()
            .map_err(|_| crate::v02::report::ResourceScopeError::CapacityExceeded)?;
        let id = crate::v02::report::ResourceScopeId::new_unique();
        let capacity = Arc::new(owner.capacity().resource_scope_for(id, options.capacity));
        let resource_scope = self.port.operation_resource_scope(id, capacity)?;
        self.track_resource_recoveries(&resource_scope, owner.groups().to_vec());
        match owner.attach_resource_scope_with_permit(resource_scope.internal_clone(), scope_permit)
        {
            Ok(()) => Ok(resource_scope),
            Err(resource_scope) => {
                resource_scope.end();
                Err(crate::v02::report::ResourceScopeError::ScopeEnded)
            }
        }
    }

    /// Blocks until manager state drains or `timeout` expires.
    ///
    /// Returns `true` only after shutdown can be finalized.
    ///
    /// # Parameters
    ///
    /// - `timeout`: maximum time to wait for tracked state to drain.
    /// - `self`: coordinator whose tracked state is awaited.
    ///
    /// # Returns
    ///
    /// `true` when shutdown can be finalized, or `false` when the timeout
    /// expires.
    pub(crate) fn shutdown_wait_for(&self, timeout: std::time::Duration) -> bool {
        let until = std::time::Instant::now().checked_add(timeout);
        let mut state = lock(&self.state);
        while !state.active_scopes.is_empty()
            || !state.service_scopes.is_empty()
            || !state.recoveries.is_empty()
        {
            if let Some(until) = until {
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
            } else {
                state = self
                    .shutdown_changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
        drop(state);
        self.finish_shutdown();
        true
    }

    /// Blocks without a timeout until all tracked manager state has drained.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator whose tracked state is awaited.
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

    /// Awaits state changes until all tracked manager state has drained.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator whose tracked state is awaited.
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

    /// Checks whether scopes, service scopes, and recoveries are all drained.
    ///
    /// # Returns
    ///
    /// `true` when no tracked scope or recovery remains.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator whose tracked state is checked.
    #[inline]
    pub(crate) fn is_shutdown_empty(&self) -> bool {
        let state = lock(&self.state);
        state.active_scopes.is_empty()
            && state.service_scopes.is_empty()
            && state.recoveries.is_empty()
    }

    /// Counts manager-lifetime resource scopes awaiting settlement.
    ///
    /// # Returns
    ///
    /// The number of tracked service resource scopes.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator whose service scopes are counted.
    #[inline]
    pub(crate) fn service_scope_count(&self) -> usize {
        lock(&self.state).service_scopes.len()
    }

    /// Counts resource recovery handles currently tracked by the manager.
    ///
    /// # Returns
    ///
    /// The number of tracked recovery handles.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator whose recovery handles are counted.
    #[inline]
    pub(crate) fn recovery_group_count(&self) -> usize {
        lock(&self.state).recoveries.len()
    }

    /// Counts tracked recovery handles that still require confirmation.
    ///
    /// # Returns
    ///
    /// The number of handles in the unconfirmed phase.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator whose recovery handles are counted.
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

    /// Reports jobs waiting in the execution port's queues.
    ///
    /// # Returns
    ///
    /// The number of queued operation jobs.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator whose execution port is queried.
    #[inline]
    pub(crate) fn queued_operation_count(&self) -> usize {
        self.port.queued_operation_count()
    }

    /// Counts cleanup jobs still running across active and service scopes.
    ///
    /// # Returns
    ///
    /// The number of cleanup jobs currently in flight.
    ///
    /// # Parameters
    ///
    /// - `self`: coordinator whose active scopes are queried.
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
}

impl ManagerCore {
    /// Tracks recovery changes for shutdown and forwards them to an optional
    /// group.
    ///
    /// # Parameters
    ///
    /// - `scope`: resource scope whose recovery events are observed.
    /// - `group`: optional group receiving the same recovery updates.
    /// - `self`: coordinator tracking the scope's recovery handles.
    fn track_resource_recoveries(
        self: &Arc<Self>,
        scope: &ResourceScope,
        groups: Vec<Arc<crate::v02::manager::group::GroupCore>>,
    ) {
        let manager = Arc::downgrade(self);
        let group_callbacks = groups.iter().map(Arc::downgrade).collect::<Vec<_>>();
        scope.set_recovery_callback(Arc::new(
            move |handle: crate::v02::resource::RecoveryHandle, active: bool| {
                for group in group_callbacks.iter().filter_map(Weak::upgrade) {
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
}

impl ManagerCore {
    /// Waits for capacity or parent-scope termination before a retry.
    ///
    /// # Parameters
    ///
    /// - `parent`: optional parent scope whose termination interrupts the wait.
    /// - `options`: limits used when waiting for capacity.
    /// - `self`: coordinator whose execution port waits for capacity.
    ///
    /// # Returns
    ///
    /// `Ok(())` when capacity becomes available, or the admission reason when
    /// the parent ends or capacity waiting fails.
    ///
    /// # Errors
    ///
    /// Returns the admission error reported by capacity waiting or parent
    /// termination.
    pub(in crate::v02::manager::core) async fn wait_for_retry_capacity(
        &self,
        target: &SubmissionTarget,
        execution: ExecutionKind,
        options: OperationOptions,
    ) -> Result<(), AdmissionError> {
        let parent = super::super::resolve_wait_parent(target)?;
        let groups = match target {
            SubmissionTarget::Root { groups } | SubmissionTarget::Parent { groups, .. } => groups,
        };
        let descendant = parent.is_some();
        let mut port_wait = self
            .port
            .wait_for_capacity(parent.clone(), execution, options);
        let mut scope_wait = parent
            .as_ref()
            .map(|scope| Box::pin(scope.wait_unavailable()));
        loop {
            let manager_generation = self.shutdown_notify.generation();
            let mut manager_changed =
                Box::pin(self.shutdown_notify.notified_since(manager_generation));
            let mut group_changed: Vec<_> = groups
                .iter()
                .map(|group| {
                    let generation = group.admission_generation();
                    Box::pin(group.admission_notified_since(generation))
                })
                .collect();

            let phase = lock(&self.state).phase;
            match (phase, descendant) {
                (ManagerPhase::Open, _) | (ManagerPhase::Draining, true) => {}
                (
                    ManagerPhase::Draining | ManagerPhase::Cancelling | ManagerPhase::Closed,
                    false,
                ) => {
                    return Err(AdmissionError::Closed);
                }
                (ManagerPhase::Cancelling | ManagerPhase::Closed, true) => {
                    return Err(AdmissionError::Stopped);
                }
            }
            if let Some(scope) = parent.as_ref() {
                if !scope.is_open() {
                    return Err(AdmissionError::ScopeEnded);
                }
                if let Err(error) = scope.context().check() {
                    return Err(super::super::map_context_error(error));
                }
            }
            let mut group_available = true;
            for group in groups {
                group_available &= group.retry_admission(descendant)?;
            }

            let capacity_result = std::future::poll_fn(|context| {
                if manager_changed.as_mut().poll(context).is_ready() {
                    return Poll::Ready(None);
                }
                if group_changed
                    .iter_mut()
                    .any(|changed| changed.as_mut().poll(context).is_ready())
                {
                    return Poll::Ready(None);
                }
                if let Some(wait) = scope_wait.as_mut()
                    && let Poll::Ready(reason) = std::pin::Pin::as_mut(wait).poll(context)
                {
                    return Poll::Ready(Some(Err(reason)));
                }
                if group_available && let Poll::Ready(result) = port_wait.as_mut().poll(context) {
                    return Poll::Ready(Some(result));
                }
                Poll::Pending
            })
            .await;
            if let Some(result) = capacity_result {
                return result;
            }
        }
    }
}

impl ManagerCore {
    #[cfg(test)]
    pub(crate) fn test_active_async_task_waker(&self) -> Option<std::task::Waker> {
        self.port.test_active_async_task_waker()
    }

    #[cfg(test)]
    pub(crate) fn record_test_value(&self, value: usize) {
        lock(&self.state).values.push(value);
    }
}

impl ManagerCore {
    #[cfg(test)]
    pub(in crate::v02::manager::core) fn test_relationships(
        &self,
    ) -> Vec<(OperationId, Option<OperationId>)> {
        lock(&self.state).relationships.clone()
    }

    #[cfg(test)]
    pub(in crate::v02::manager::core) fn test_values(&self) -> Vec<usize> {
        lock(&self.state).values.clone()
    }
}

impl ManagerCore {
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
            16,
        );
        let root = core.root_view();
        (core, root)
    }
}

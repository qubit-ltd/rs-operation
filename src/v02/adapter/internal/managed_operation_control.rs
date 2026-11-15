// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use super::ManagedFacts;
use super::lock;
use super::wake_all;
use crate::v02::operation::OperationCore;
use crate::v02::operation::StopControl;
use crate::v02::protocol::OperationControl;
use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::ChildRelease;
use crate::v02::report::ChildSnapshot;
use crate::v02::report::SettlementReport;
use crate::v02::report::StopReason;
use crate::v02::report::StopReport;
use crate::v02::scope::ScopeCore;

/// Manager-owned observation bridge for one real operation scope.
///
/// Outcome and input release are mirrored here because `OperationCore` exposes
/// only untyped outcome availability. Settlement remains owned by
/// `OperationCore`; this bridge only wakes settlement observers after the core
/// publishes its immutable report.
pub(crate) struct ManagedOperationControl {
    /// Core operation whose immutable settlement report is observed.
    operation: Arc<OperationCore>,
    /// Scope used to propagate stop requests and input-release evidence.
    scope: Arc<ScopeCore>,
    /// Child outcome and release facts plus their registered observers.
    facts: Mutex<ManagedFacts>,
}
impl ManagedOperationControl {
    /// Creates a bridge for child facts and its owning operation scope.
    ///
    /// # Parameters
    ///
    /// * `operation` owns the child operation's immutable settlement report.
    /// * `scope` receives stop requests and input-release evidence.
    pub(crate) fn new(operation: Arc<OperationCore>, scope: Arc<ScopeCore>) -> Arc<Self> {
        Arc::new(Self {
            operation,
            scope,
            facts: Mutex::new(ManagedFacts {
                outcome: None,
                release: None,
                outcome_waiters: Vec::new(),
                release_waiters: Vec::new(),
                settlement_waiters: Vec::new(),
                settlement_generation: 0,
            }),
        })
    }

    /// Publishes the first matching outcome and wakes its observers.
    ///
    /// # Parameters
    ///
    /// * `outcome` is published only when it belongs to this operation.
    pub(crate) fn publish_outcome(&self, outcome: ChildOutcomeSummary) -> bool {
        let waiters = {
            let mut facts = lock(&self.facts);
            if outcome.operation_id != self.operation.id() || facts.outcome.is_some() {
                return false;
            }
            facts.outcome = Some(outcome);
            std::mem::take(&mut facts.outcome_waiters)
        };
        wake_all(waiters);
        true
    }

    /// Publishes release evidence and informs the parent scope.
    ///
    /// # Parameters
    ///
    /// * `release` confirms the child has ended use of its input.
    pub(crate) fn confirm_input_release(&self, release: ChildRelease) -> bool {
        let waiters = {
            let mut facts = lock(&self.facts);
            if release.operation_id != self.operation.id() || facts.release.is_some() {
                return false;
            }
            facts.release = Some(release);
            std::mem::take(&mut facts.release_waiters)
        };
        if let Some(parent) = self.scope.parent() {
            parent.child_input_released(self.operation.id());
        }
        wake_all(waiters);
        true
    }

    /// Wakes settlement observers after the core publishes its report.
    pub(crate) fn settlement_published(&self) {
        let waiters = {
            let mut facts = lock(&self.facts);
            facts.settlement_generation = facts.settlement_generation.wrapping_add(1);
            std::mem::take(&mut facts.settlement_waiters)
        };
        wake_all(waiters);
    }

    /// Polls the mirrored outcome and registers the current waker if pending.
    ///
    /// # Parameters
    ///
    /// * `cx` supplies the observer waker to register while the outcome is
    ///   pending.
    fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
        poll_managed_fact(
            &self.facts,
            cx,
            |facts| &mut facts.outcome_waiters,
            |facts| facts.outcome.clone(),
        )
    }

    /// Polls confirmed input release and registers the current waker if
    /// pending.
    ///
    /// # Parameters
    ///
    /// * `cx` supplies the observer waker to register while release is pending.
    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        poll_managed_fact(
            &self.facts,
            cx,
            |facts| &mut facts.release_waiters,
            |facts| facts.release.clone(),
        )
    }

    /// Polls core settlement with a generation check to avoid missed wakeups.
    ///
    /// # Parameters
    ///
    /// * `cx` supplies the observer waker registered for settlement
    ///   publication.
    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>> {
        loop {
            let generation = lock(&self.facts).settlement_generation;
            if let Some(report) = self.operation.snapshot().settlement {
                return Poll::Ready(report);
            }
            let candidate = cx.waker().clone();
            let (retry, retired) = {
                let mut facts = lock(&self.facts);
                if facts.settlement_generation != generation {
                    (true, Some(candidate))
                } else if let Some(index) = facts
                    .settlement_waiters
                    .iter()
                    .position(|old| old.will_wake(&candidate))
                {
                    (
                        false,
                        Some(std::mem::replace(&mut facts.settlement_waiters[index], candidate)),
                    )
                } else {
                    facts.settlement_waiters.push(candidate);
                    (false, None)
                }
            };
            drop(retired);
            if retry {
                continue;
            }
            return Poll::Pending;
        }
    }
}

impl OperationControl for ManagedOperationControl {
    /// Forwards a stop request to the owning scope.
    ///
    /// # Parameters
    ///
    /// * `reason` identifies why the parent asks the operation to stop.
    fn request_stop(&self, reason: StopReason) {
        self.scope.request_stop(reason);
    }

    /// Combines the core phase, scope stop reason and mirrored child facts.
    fn snapshot(&self) -> ChildSnapshot {
        let operation = self.operation.snapshot();
        let facts = lock(&self.facts);
        ChildSnapshot {
            operation_id: operation.operation_id,
            execution: operation.execution,
            stop: operation.stop.or_else(|| {
                self.scope.context().stop_reason().map(|reason| StopReport {
                    reason,
                    accepted_at: None,
                })
            }),
            outcome: facts.outcome.clone(),
            input_released: facts.release.is_some(),
            settlement: operation.settlement,
        }
    }

    /// Registers or resolves an observer for the child's outcome.
    ///
    /// # Parameters
    ///
    /// * `cx` supplies the observer waker if the outcome is pending.
    fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
        ManagedOperationControl::poll_outcome(self, cx)
    }

    /// Registers or resolves an observer for child input release.
    ///
    /// # Parameters
    ///
    /// * `cx` supplies the observer waker if release evidence is pending.
    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        ManagedOperationControl::poll_release(self, cx)
    }

    /// Registers or resolves an observer for the core settlement report.
    ///
    /// # Parameters
    ///
    /// * `cx` supplies the observer waker if settlement is pending.
    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>> {
        ManagedOperationControl::poll_settlement(self, cx)
    }
}

impl StopControl for ManagedOperationControl {
    /// Routes the operation stop request through the shared control contract.
    ///
    /// # Parameters
    ///
    /// * `reason` identifies why the parent asks the operation to stop.
    fn request_stop(&self, reason: StopReason) {
        OperationControl::request_stop(self, reason);
    }
}

/// Polls one managed fact and registers a waker without losing concurrent
/// updates.
///
/// # Type Parameters
///
/// * `V` is the cloneable fact returned when it has been published.
///
/// # Parameters
///
/// * `state` stores the mirrored facts and their observer lists.
/// * `cx` supplies the observer waker for a pending fact.
/// * `waiters` selects the waker list associated with the requested fact.
/// * `value` reads that fact when publication is complete.
fn poll_managed_fact<V: Clone>(
    state: &Mutex<ManagedFacts>,
    cx: &mut Context<'_>,
    waiters: impl for<'a> Fn(&'a mut ManagedFacts) -> &'a mut Vec<Waker>,
    value: impl Fn(&ManagedFacts) -> Option<V>,
) -> Poll<V> {
    if let Some(value) = value(&lock(state)) {
        return Poll::Ready(value);
    }
    let candidate = cx.waker().clone();
    let (result, retired) = {
        let mut facts = lock(state);
        if let Some(value) = value(&facts) {
            (Poll::Ready(value), Some(candidate))
        } else {
            let waiters = waiters(&mut facts);
            if let Some(index) = waiters.iter().position(|old| old.will_wake(&candidate)) {
                (Poll::Pending, Some(std::mem::replace(&mut waiters[index], candidate)))
            } else {
                waiters.push(candidate);
                (Poll::Pending, None)
            }
        }
    };
    drop(retired);
    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    use super::ManagedOperationControl;
    use crate::v02::context::OperationContext;
    use crate::v02::manager::capacity::ManagerCapacityPools;
    use crate::v02::manager::capacity::ScopeCapacity;
    use crate::v02::manager::core::ManagerCore;
    use crate::v02::operation::OperationCore;
    use crate::v02::protocol::OperationControl;
    use crate::v02::report::ChildOutcomeKind;
    use crate::v02::report::ChildOutcomeSummary;
    use crate::v02::report::ChildRelease;
    use crate::v02::report::CleanupFailure;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::EffectRecord;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ManagerLimits;
    use crate::v02::report::OperationId;
    use crate::v02::report::OperationOptions;
    use crate::v02::report::ResourceCleanupReport;
    use crate::v02::report::ResourceScopeId;
    use crate::v02::report::ScopeLimits;
    use crate::v02::report::SettlementReport;
    use crate::v02::report::SettlementStatus;
    use crate::v02::resource::ResourceScope;
    use crate::v02::resource::cleanup_port::CleanupDispatch;
    use crate::v02::resource::cleanup_port::CleanupDispatchPermit;
    use crate::v02::resource::cleanup_port::CleanupJob;
    use crate::v02::resource::cleanup_port::CleanupQueueError;
    use crate::v02::scope::ScopeCore;
    use crate::v02::scope::ScopeCoreInit;

    struct NoopCleanup;

    impl CleanupDispatch for NoopCleanup {
        fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
            Ok(Box::new(NoopPermit))
        }
    }

    struct NoopPermit;

    impl CleanupDispatchPermit for NoopPermit {
        fn enqueue(self: Box<Self>, job: Box<dyn CleanupJob>) {
            drop(job);
        }
    }

    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn make_scope(id: OperationId, parent: Option<Arc<ScopeCore>>) -> Arc<ScopeCore> {
        let pools = ManagerCapacityPools::new(ManagerLimits::default());
        let capacity = Arc::new(ScopeCapacity::root_for_operation(&pools, id, ScopeLimits::default()));
        let resources = ResourceScope::new(
            ManagerId::new_unique(),
            ResourceScopeId::new_unique(),
            Arc::clone(&capacity),
            Arc::new(NoopCleanup),
        );
        let context = match parent.as_ref() {
            Some(parent) => parent.context().child(Default::default()).unwrap(),
            None => OperationContext::new(Default::default()).unwrap(),
        };
        ScopeCore::new(ScopeCoreInit {
            id,
            manager: std::sync::Weak::<ManagerCore>::new(),
            parent: parent.as_ref().map(Arc::downgrade),
            context,
            options: OperationOptions::default(),
            diagnostic_limit: 8,
            capacity,
            resource_scope: resources,
            group: None,
        })
    }

    fn make_control() -> (Arc<OperationCore>, Arc<ManagedOperationControl>) {
        let id = OperationId::new_unique();
        let operation = OperationCore::new(id);
        let control = ManagedOperationControl::new(Arc::clone(&operation), make_scope(id, None));
        (operation, control)
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
            cleanup_failures: Arc::<[CleanupFailure]>::from([]),
            cleanup_failures_total: 0,
            cleanup_failures_truncated: false,
            resource_reports: Arc::<[ResourceCleanupReport]>::from([]),
            resource_reports_total: 0,
            resource_reports_truncated: false,
            transfers: Arc::from([]),
            transfers_total: 0,
            transfers_truncated: false,
            recoveries: Arc::from([]),
            recoveries_total: 0,
            recoveries_truncated: false,
            effects: Arc::<[EffectRecord]>::from([]),
            effects_total: 0,
            effect_overflow: 0,
        }
    }

    #[test]
    fn test_managed_control_publishes_matching_outcome_and_release_once() {
        let (_operation, control) = make_control();
        let id = control.snapshot().operation_id;
        let other_id = OperationId::new_unique();
        let wake_count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_count));
        let mut cx = Context::from_waker(&waker);

        assert!(control.poll_outcome(&mut cx).is_pending());
        assert!(control.poll_release(&mut cx).is_pending());
        let wrong_outcome = ChildOutcomeSummary {
            operation_id: other_id,
            kind: ChildOutcomeKind::Succeeded,
            summary: DiagnosticSummary::new("must be rejected"),
        };
        assert!(!control.publish_outcome(wrong_outcome));
        assert!(!control.snapshot().input_released);
        assert_eq!(wake_count.0.load(Ordering::SeqCst), 0);

        let outcome = ChildOutcomeSummary {
            operation_id: id,
            kind: ChildOutcomeKind::Succeeded,
            summary: DiagnosticSummary::new("completed"),
        };
        assert!(control.publish_outcome(outcome.clone()));
        assert!(!control.publish_outcome(outcome.clone()));
        assert_eq!(control.poll_outcome(&mut cx), Poll::Ready(outcome));
        assert_eq!(wake_count.0.load(Ordering::SeqCst), 1);

        assert!(!control.confirm_input_release(ChildRelease {
            operation_id: other_id,
            confirmed_at: None,
        }));
        assert!(!control.snapshot().input_released);
        let release = ChildRelease {
            operation_id: id,
            confirmed_at: None,
        };
        assert!(control.confirm_input_release(release.clone()));
        assert!(!control.confirm_input_release(release.clone()));
        match control.poll_release(&mut cx) {
            Poll::Ready(published) => assert_eq!(published.operation_id, release.operation_id),
            Poll::Pending => panic!("confirmed release should be ready"),
        }
        assert!(control.snapshot().input_released);
        assert_eq!(wake_count.0.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_control_trait_routes_stop_and_observer_polls() {
        use crate::v02::operation::StopControl;

        let (operation, control) = make_control();
        let control_trait: &dyn OperationControl = control.as_ref();
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert!(control_trait.poll_outcome(&mut cx).is_pending());
        assert!(control_trait.poll_release(&mut cx).is_pending());
        assert!(control_trait.poll_settlement(&mut cx).is_pending());
        control_trait.request_stop(crate::v02::report::StopReason::UserCancelled);
        assert!(matches!(
            control_trait.snapshot().stop,
            Some(stop) if stop.reason == crate::v02::report::StopReason::UserCancelled
        ));
        StopControl::request_stop(control.as_ref(), crate::v02::report::StopReason::Timeout);
        assert_eq!(
            control_trait.snapshot().stop.unwrap().reason,
            crate::v02::report::StopReason::UserCancelled,
            "the first stop request must remain authoritative"
        );
        assert_eq!(operation.id(), control_trait.snapshot().operation_id);
    }

    #[test]
    fn test_input_release_is_forwarded_to_parent_scope() {
        use crate::v02::report::ChildRequirement;

        let parent_id = OperationId::new_unique();
        let parent = make_scope(parent_id, None);
        let child_id = OperationId::new_unique();
        let child_scope = make_scope(child_id, Some(Arc::clone(&parent)));
        assert!(parent.mark_child(child_id, Arc::downgrade(&child_scope), ChildRequirement::Required,));
        let operation = OperationCore::new(child_id);
        let control = ManagedOperationControl::new(operation, child_scope);

        assert!(!parent.child_input_has_released(child_id));
        assert!(control.confirm_input_release(ChildRelease {
            operation_id: child_id,
            confirmed_at: None,
        }));
        assert!(parent.child_input_has_released(child_id));
    }

    #[test]
    fn test_managed_control_settlement_waits_for_core_publication() {
        let (operation, control) = make_control();
        let id = operation.id();
        let wake_count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_count));
        let mut cx = Context::from_waker(&waker);

        assert!(control.poll_settlement(&mut cx).is_pending());
        assert_eq!(wake_count.0.load(Ordering::SeqCst), 0);
        let report = operation.publish_settlement(settlement(id)).unwrap();
        assert!(control.poll_settlement(&mut cx).is_ready());
        assert_eq!(wake_count.0.load(Ordering::SeqCst), 0);

        control.settlement_published();
        assert_eq!(wake_count.0.load(Ordering::SeqCst), 1);
        match control.poll_settlement(&mut cx) {
            Poll::Ready(published) => assert!(Arc::ptr_eq(&published, &report)),
            Poll::Pending => panic!("published settlement should be ready"),
        }
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Driver and shared state for externally executed operations.

use std::future::Future;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;

use super::ChildOutcomeSummary;
use super::DiagnosticSummary;
use super::ExternalExecutionContext;
use super::ManagedOperationControl;
use super::OperationOutcome;
use super::OperationScope;
use super::child_outcome_summary;
use super::dispose_panic_payload;
use super::lock;
use super::panic_message;
use crate::v02::report::ExternalProtocolFault;
use crate::v02::report::ExternalProtocolFaultKind;
use crate::v02::report::ExternalProtocolPort;
use crate::v02::report::OperationId;

/// Records one bounded diagnostic for an external protocol port.
///
/// # Parameters
///
/// - `state`: Shared execution context slot used to reach the operation core.
/// - `operation_id`: Identity of the operation that owns the protocol.
/// - `port`: External protocol port that failed.
/// - `kind`: Category of the protocol fault.
/// - `observed_operation_id`: Identity returned by a mismatched port, if any.
/// - `summary`: Fixed summary describing the fault.
fn record_protocol_fault(
    state: &Arc<Mutex<Option<Arc<ExternalExecutionContext>>>>,
    operation_id: OperationId,
    port: ExternalProtocolPort,
    kind: ExternalProtocolFaultKind,
    observed_operation_id: Option<OperationId>,
    summary: &str,
) {
    let context = lock(state).as_ref().cloned();
    if let Some(context) = context {
        context.record_protocol_fault(ExternalProtocolFault {
            operation_id,
            port,
            kind,
            observed_operation_id,
            summary: DiagnosticSummary::new(summary),
        });
    }
}

/// Shared one-shot callback for publishing a typed external operation outcome.
///
/// # Type Parameters
///
/// - `T`: Successful value produced by the external task.
/// - `E`: Error value produced by the external task.
pub type ExternalOutcomePublisher<T, E> = Arc<Mutex<Option<Box<dyn FnOnce(OperationOutcome<T, E>) + Send + 'static>>>>;

/// Shared result of claiming an external task for its operation identity.
pub type ExternalBindResult = Arc<Mutex<Option<Result<(), crate::v02::report::ExecutionFailure>>>>;

/// Drives an external task until its outcome and required cleanup are observed.
///
/// # Type Parameters
///
/// - `T`: Successful value produced by the external task.
/// - `E`: Error value produced by the external task.
/// - `P`: Prepared external task implementation.
///
/// # Parameters
///
/// - `scope`: Manager-owned scope assigned to the task.
/// - `adapter`: Prepared task being activated and polled.
/// - `bind_state`: Shared result of the task's identity claim.
/// - `managed_state`: Shared manager control installed before dispatch.
/// - `execution_state`: Shared execution context used to finalize the task.
/// - `publisher_state`: One-shot callback that receives the task outcome.
/// - `activation_permitted`: Whether manager lifecycle state permits starting.
pub async fn drive_external<T, E, P>(
    scope: OperationScope,
    adapter: Arc<P>,
    bind_state: ExternalBindResult,
    managed_state: Arc<Mutex<Option<Arc<ManagedOperationControl>>>>,
    execution_state: Arc<Mutex<Option<Arc<ExternalExecutionContext>>>>,
    publisher_state: ExternalOutcomePublisher<T, E>,
    activation_permitted: bool,
) where
    T: Send + 'static,
    E: Send + 'static,
    P: crate::v02::adapter::PreparedExternalOperation<T, E>,
{
    let operation_id = scope.id();
    let scope_core_for_settlement = Arc::clone(&scope.core);
    let context = scope.context();
    let bind = lock(&bind_state)
        .take()
        .expect("accepted external operation has a bind result");
    let mut activated = false;
    let mut typed = if let Err(failure) = bind {
        if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| adapter.clone().abandon())) {
            dispose_panic_payload(payload);
        }
        Some(OperationOutcome::ExecutionFailed(failure))
    } else if !activation_permitted {
        if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| adapter.clone().abandon())) {
            dispose_panic_payload(payload);
        }
        Some(OperationOutcome::Stopped(crate::v02::report::StopReport {
            reason: context
                .stop_reason()
                .unwrap_or(crate::v02::report::StopReason::ParentStopped),
            accepted_at: None,
        }))
    } else {
        match panic::catch_unwind(AssertUnwindSafe(|| adapter.snapshot())) {
            Err(payload) => {
                if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| adapter.clone().abandon())) {
                    dispose_panic_payload(payload);
                }
                let summary = DiagnosticSummary::new(&panic_message(payload.as_ref()));
                dispose_panic_payload(payload);
                Some(OperationOutcome::ExecutionFailed(
                    crate::v02::report::ExecutionFailure {
                        operation_id,
                        summary,
                        source: None,
                    },
                ))
            }
            Ok(snapshot) if snapshot.operation_id != operation_id => {
                if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| adapter.clone().abandon())) {
                    dispose_panic_payload(payload);
                }
                Some(OperationOutcome::ExecutionFailed(
                    crate::v02::report::ExecutionFailure {
                        operation_id,
                        summary: DiagnosticSummary::new("external snapshot identity mismatch"),
                        source: None,
                    },
                ))
            }
            Ok(_) => match panic::catch_unwind(AssertUnwindSafe(|| adapter.clone().activate(scope))) {
                Ok(Ok(())) => {
                    activated = true;
                    None
                }
                Ok(Err(failure)) => Some(OperationOutcome::ExecutionFailed(failure)),
                Err(payload) => {
                    let summary = DiagnosticSummary::new(&panic_message(payload.as_ref()));
                    dispose_panic_payload(payload);
                    Some(OperationOutcome::ExecutionFailed(
                        crate::v02::report::ExecutionFailure {
                            operation_id,
                            summary,
                            source: None,
                        },
                    ))
                }
            },
        }
    };
    let managed = lock(&managed_state)
        .as_ref()
        .cloned()
        .expect("manager control is installed before dispatch");
    let finish_execution = || {
        if let Some(context) = lock(&execution_state).as_ref() {
            context.finish();
        }
    };
    let mut published = false;
    let mut summary: Option<ChildOutcomeSummary> = None;
    let mut release = false;
    let mut release_polled = false;
    let mut settlement: Option<Arc<crate::v02::report::SettlementReport>> = None;
    let mut typed_poll_done = typed.is_some();
    let mut stop_signal = Box::pin(context.stop_requested());
    let mut stop_forwarded = false;
    let mut summary_poll_done = !activated;
    let mut summary_poll_failed = false;
    let mut typed_poll_failed = false;
    let mut release_poll_failed = false;
    let mut settlement_poll_failed = false;
    if activated && let Some(reason) = context.stop_reason() {
        if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| adapter.request_stop(reason))) {
            dispose_panic_payload(payload);
        }
        stop_forwarded = true;
    }
    if typed_poll_done && let Some(callback) = lock(&publisher_state).take() {
        callback(typed.take().expect("outcome exists"));
        published = true;
        finish_execution();
    }
    std::future::poll_fn(|cx| {
        if activated
            && !stop_forwarded
            && let Poll::Ready(reason) = stop_signal.as_mut().poll(cx)
        {
            if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| adapter.request_stop(reason))) {
                dispose_panic_payload(payload);
            }
            stop_forwarded = true;
        }
        if activated && !typed_poll_done {
            match panic::catch_unwind(AssertUnwindSafe(|| adapter.poll_take_outcome(cx))) {
                Ok(Poll::Ready(outcome)) => {
                    typed = Some(outcome);
                    typed_poll_done = true;
                }
                Ok(Poll::Pending) => {}
                Err(payload) => {
                    let summary = DiagnosticSummary::new(&panic_message(payload.as_ref()));
                    dispose_panic_payload(payload);
                    typed = Some(OperationOutcome::ExecutionFailed(
                        crate::v02::report::ExecutionFailure {
                            operation_id,
                            summary,
                            source: None,
                        },
                    ));
                    typed_poll_done = true;
                    typed_poll_failed = true;
                }
            }
        }
        if activated && !summary_poll_done {
            match panic::catch_unwind(AssertUnwindSafe(|| adapter.poll_outcome(cx))) {
                Ok(Poll::Ready(value)) => {
                    summary = Some(value);
                    summary_poll_done = true;
                }
                Ok(Poll::Pending) => {}
                Err(payload) => {
                    summary_poll_done = true;
                    summary_poll_failed = true;
                    dispose_panic_payload(payload);
                }
            }
        }
        if !release_polled && !release_poll_failed {
            match panic::catch_unwind(AssertUnwindSafe(|| adapter.poll_release(cx))) {
                Ok(Poll::Ready(value)) => {
                    release_polled = true;
                    if value.operation_id != operation_id {
                        record_protocol_fault(
                            &execution_state,
                            operation_id,
                            ExternalProtocolPort::Release,
                            ExternalProtocolFaultKind::IdentityMismatch,
                            Some(value.operation_id),
                            "external release identity mismatch",
                        );
                    }
                    release = managed.confirm_input_release(value);
                }
                Ok(Poll::Pending) => {}
                Err(payload) => {
                    release_poll_failed = true;
                    record_protocol_fault(
                        &execution_state,
                        operation_id,
                        ExternalProtocolPort::Release,
                        ExternalProtocolFaultKind::Panicked,
                        None,
                        "external release poll panicked",
                    );
                    dispose_panic_payload(payload);
                }
            }
        }
        if settlement.is_none() && !settlement_poll_failed {
            match panic::catch_unwind(AssertUnwindSafe(|| adapter.poll_settlement(cx))) {
                Ok(Poll::Ready(value)) if value.operation_id == operation_id => settlement = Some(value),
                Ok(Poll::Ready(value)) => {
                    settlement_poll_failed = true;
                    record_protocol_fault(
                        &execution_state,
                        operation_id,
                        ExternalProtocolPort::Settlement,
                        ExternalProtocolFaultKind::IdentityMismatch,
                        Some(value.operation_id),
                        "external settlement identity mismatch",
                    );
                }
                Ok(Poll::Pending) => {}
                Err(payload) => {
                    settlement_poll_failed = true;
                    record_protocol_fault(
                        &execution_state,
                        operation_id,
                        ExternalProtocolPort::Settlement,
                        ExternalProtocolFaultKind::Panicked,
                        None,
                        "external settlement poll panicked",
                    );
                    dispose_panic_payload(payload);
                }
            }
        }
        if typed_poll_done && !published && (!activated || summary_poll_done || typed_poll_failed) {
            if activated {
                let expected = typed.as_ref().map(|value| child_outcome_summary(operation_id, value));
                if summary_poll_failed
                    || summary.as_ref().is_none_or(|value| {
                        value.operation_id != operation_id
                            || expected.as_ref().is_none_or(|expected| value.kind != expected.kind)
                    })
                {
                    typed = Some(OperationOutcome::ExecutionFailed(
                        crate::v02::report::ExecutionFailure {
                            operation_id,
                            summary: DiagnosticSummary::new("external outcome summary mismatch"),
                            source: None,
                        },
                    ));
                }
            }
            if let Some(callback) = lock(&publisher_state).take()
                && let Some(outcome) = typed.take()
            {
                callback(outcome);
                finish_execution();
                published = true;
            }
        }
        if published && release && settlement.is_some() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    scope_core_for_settlement.finish_external_driver_settlement(
        settlement.expect("external driver completed after settlement was observed"),
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    use super::super::SubmissionLease;
    use super::ChildOutcomeSummary;
    use super::DiagnosticSummary;
    use super::ExternalExecutionContext;
    use super::ExternalOutcomePublisher;
    use super::ManagedOperationControl;
    use super::OperationOutcome;
    use super::drive_external;
    use crate::v02::adapter::ExternalBindError;
    use crate::v02::adapter::PreparedExternalOperation;
    use crate::v02::context::OperationContext;
    use crate::v02::operation::OperationCore;
    use crate::v02::operation::OperationHandle;
    use crate::v02::operation::OutcomeSlot;
    use crate::v02::operation::StopControl;
    use crate::v02::protocol::OperationControl;
    use crate::v02::report::ChildOutcomeKind;
    use crate::v02::report::ChildRelease;
    use crate::v02::report::ChildSnapshot;
    use crate::v02::report::ContextOptions;
    use crate::v02::report::EffectFact;
    use crate::v02::report::EffectRecord;
    use crate::v02::report::ExecutionFailure;
    use crate::v02::report::ExecutionPhase;
    use crate::v02::report::OperationOptions;
    use crate::v02::report::SettlementReport;
    use crate::v02::report::SettlementStatus;
    use crate::v02::report::StopReason;
    use crate::v02::scope::OperationScope;
    use crate::v02::scope::ScopeCore;
    use crate::v02::scope::ScopeCoreInit;

    #[derive(Default)]
    struct LifecycleLease {
        execution_finished: AtomicUsize,
        settlement_finished: AtomicUsize,
        outcome_delivered: AtomicUsize,
    }

    impl super::super::SubmissionLease for LifecycleLease {
        fn execution_finished(&self) {
            self.execution_finished.fetch_add(1, Ordering::SeqCst);
        }

        fn settlement_finished(&self) {
            self.settlement_finished.fetch_add(1, Ordering::SeqCst);
        }

        fn outcome_delivered(&self, _delivery: super::super::OutcomeDelivery) {
            self.outcome_delivered.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct SummaryGateAdapter {
        id: crate::v02::report::OperationId,
        activations: AtomicUsize,
        stop_requests: AtomicUsize,
        abandon_calls: AtomicUsize,
        summary_ready: AtomicBool,
        summary_waker: Mutex<Option<Waker>>,
        outcome_polls: AtomicUsize,
        summary_polls: AtomicUsize,
        release_polls: AtomicUsize,
        settlement_polls: AtomicUsize,
    }

    impl SummaryGateAdapter {
        fn release_summary(&self) {
            self.summary_ready.store(true, Ordering::SeqCst);
            let waker = self.summary_waker.lock().unwrap().take();
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    impl OperationControl for SummaryGateAdapter {
        fn request_stop(&self, _reason: StopReason) {
            self.stop_requests.fetch_add(1, Ordering::SeqCst);
        }

        fn snapshot(&self) -> ChildSnapshot {
            ChildSnapshot {
                operation_id: self.id,
                execution: ExecutionPhase::Queued,
                stop: None,
                outcome: None,
                input_released: false,
                settlement: None,
            }
        }

        fn poll_outcome(&self, context: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
            self.summary_polls.fetch_add(1, Ordering::SeqCst);
            if !self.summary_ready.load(Ordering::SeqCst) {
                *self.summary_waker.lock().unwrap() = Some(context.waker().clone());
                return Poll::Pending;
            }
            Poll::Ready(ChildOutcomeSummary {
                operation_id: self.id,
                kind: ChildOutcomeKind::Succeeded,
                summary: DiagnosticSummary::new("manual driver result"),
            })
        }

        fn poll_release(&self, _context: &mut Context<'_>) -> Poll<ChildRelease> {
            self.release_polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(ChildRelease {
                operation_id: self.id,
                confirmed_at: None,
            })
        }

        fn poll_settlement(&self, _context: &mut Context<'_>) -> Poll<Arc<SettlementReport>> {
            self.settlement_polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Arc::new(SettlementReport {
                operation_id: self.id,
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
                effects: Arc::from([EffectRecord {
                    operation_id: self.id,
                    resource_id: None,
                    fact: EffectFact::Known,
                    description: DiagnosticSummary::new("manual driver effect"),
                }]),
                effects_total: 1,
                effect_overflow: 0,
                external_settlement: None,
            }))
        }
    }

    impl PreparedExternalOperation<usize, ()> for SummaryGateAdapter {
        fn bind(&self, _id: crate::v02::report::OperationId) -> Result<(), ExternalBindError> {
            Ok(())
        }

        fn activate(self: Arc<Self>, _scope: OperationScope) -> Result<(), ExecutionFailure> {
            self.activations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn abandon(self: Arc<Self>) {
            self.abandon_calls.fetch_add(1, Ordering::SeqCst);
        }

        fn poll_take_outcome(&self, _context: &mut Context<'_>) -> Poll<OperationOutcome<usize, ()>> {
            self.outcome_polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(OperationOutcome::Succeeded(7))
        }
    }

    struct WakeCounter(AtomicUsize);

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_external_summary_gate_manual_driver_poll() {
        let manager = super::super::ManagerCore::new(
            Arc::new(super::super::TestPort),
            OperationContext::new(ContextOptions::default()).unwrap(),
            crate::v02::report::ScopeLimits::default(),
        );
        let id = crate::v02::report::OperationId::new_unique();
        let options = OperationOptions::default();
        let (capacity, resource_scope) = super::super::test_resources(id, None, &options);
        let scope_core = ScopeCore::new(ScopeCoreInit {
            id,
            manager: Arc::downgrade(&manager),
            parent: None,
            context: OperationContext::new(ContextOptions::default()).unwrap(),
            options,
            diagnostic_limit: 8,
            capacity,
            resource_scope,
            groups: Vec::new(),
        });
        let scope = OperationScope::new(Arc::clone(&scope_core), Arc::clone(&manager));
        let operation = OperationCore::new(id);
        scope_core.bind_operation_core(&operation);
        let lease_impl = Arc::new(LifecycleLease::default());
        let lease: Arc<dyn super::super::SubmissionLease> = lease_impl.clone();
        let managed = ManagedOperationControl::new(Arc::clone(&operation), Arc::clone(&scope_core));
        manager.register_settlement(
            Arc::clone(&scope_core),
            Arc::clone(&operation),
            Arc::clone(&lease),
            Arc::clone(&managed),
        );
        let slot = Arc::new(OutcomeSlot::<usize, ()>::new(Arc::clone(&operation)));
        slot.set_delivery_hook({
            let lease_for_delivery = Arc::clone(&lease_impl);
            move |delivery| lease_for_delivery.outcome_delivered(delivery)
        });
        let mut handle = OperationHandle::new(
            Arc::clone(&operation),
            Arc::clone(&slot),
            Arc::clone(&managed) as Arc<dyn StopControl>,
        );
        let execution = Arc::new(ExternalExecutionContext::new(
            Arc::clone(&operation),
            Arc::clone(&lease),
            Arc::clone(&scope_core),
            None,
        ));
        let execution_state = Arc::new(Mutex::new(Some(execution)));
        let managed_state = Arc::new(Mutex::new(Some(managed)));
        let adapter = Arc::new(SummaryGateAdapter {
            id,
            activations: AtomicUsize::new(0),
            stop_requests: AtomicUsize::new(0),
            abandon_calls: AtomicUsize::new(0),
            summary_ready: AtomicBool::new(false),
            summary_waker: Mutex::new(None),
            outcome_polls: AtomicUsize::new(0),
            summary_polls: AtomicUsize::new(0),
            release_polls: AtomicUsize::new(0),
            settlement_polls: AtomicUsize::new(0),
        });
        let publisher_calls = Arc::new(AtomicUsize::new(0));
        let publisher: ExternalOutcomePublisher<usize, ()> = Arc::new(Mutex::new(Some(Box::new({
            let publisher_calls = Arc::clone(&publisher_calls);
            let slot = Arc::clone(&slot);
            move |outcome| {
                publisher_calls.fetch_add(1, Ordering::SeqCst);
                assert!(slot.publish(outcome).is_ok());
            }
        }))));
        let bind_state = Arc::new(Mutex::new(Some(Ok(()))));
        adapter
            .bind(id)
            .expect("external bind preflight succeeds before driver starts");
        let mut driver = Box::pin(drive_external(
            scope,
            Arc::clone(&adapter),
            bind_state,
            managed_state,
            execution_state,
            publisher,
            true,
        ));
        let wake_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_counter));
        let mut context = Context::from_waker(&waker);

        assert!(driver.as_mut().poll(&mut context).is_pending());
        assert_eq!(adapter.outcome_polls.load(Ordering::SeqCst), 1);
        assert_eq!(adapter.summary_polls.load(Ordering::SeqCst), 1);
        assert_eq!(adapter.release_polls.load(Ordering::SeqCst), 1);
        assert_eq!(adapter.settlement_polls.load(Ordering::SeqCst), 1);
        assert_eq!(publisher_calls.load(Ordering::SeqCst), 0);
        assert!(adapter.summary_waker.lock().unwrap().is_some());

        adapter.release_summary();
        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 1);
        assert!(driver.as_mut().poll(&mut context).is_ready());
        assert_eq!(publisher_calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            handle.try_take_outcome().unwrap(),
            Some(OperationOutcome::Succeeded(7))
        ));
        assert_eq!(lease_impl.execution_finished.load(Ordering::SeqCst), 1);
        assert!(scope_core.external_settlement().is_some());
        scope_core.end();
        assert!(matches!(
            handle.wait_settlement_for(std::time::Duration::ZERO).unwrap(),
            crate::v02::report::SettlementWait::Ready(_)
        ));
        assert_eq!(lease_impl.settlement_finished.load(Ordering::SeqCst), 1);
        assert_eq!(lease_impl.outcome_delivered.load(Ordering::SeqCst), 1);

        manager.finish_shutdown();
    }

    fn manual_scope() -> (
        Arc<super::super::ManagerCore>,
        Arc<ScopeCore>,
        OperationScope,
        crate::v02::report::OperationId,
    ) {
        let manager = super::super::ManagerCore::new(
            Arc::new(super::super::TestPort),
            OperationContext::new(ContextOptions::default()).unwrap(),
            crate::v02::report::ScopeLimits::default(),
        );
        let id = crate::v02::report::OperationId::new_unique();
        let options = OperationOptions::default();
        let (capacity, resource_scope) = super::super::test_resources(id, None, &options);
        let scope_core = ScopeCore::new(ScopeCoreInit {
            id,
            manager: Arc::downgrade(&manager),
            parent: None,
            context: OperationContext::new(ContextOptions::default()).unwrap(),
            options,
            diagnostic_limit: 8,
            capacity,
            resource_scope,
            groups: Vec::new(),
        });
        let scope = OperationScope::new(Arc::clone(&scope_core), Arc::clone(&manager));
        (manager, scope_core, scope, id)
    }

    #[test]
    fn test_external_driver_forwards_real_scope_stop_before_polling() {
        let (manager, scope_core, scope, id) = manual_scope();
        let operation = OperationCore::new(id);
        scope_core.bind_operation_core(&operation);
        let lease_impl = Arc::new(LifecycleLease::default());
        let lease: Arc<dyn super::super::SubmissionLease> = lease_impl.clone();
        let managed = ManagedOperationControl::new(Arc::clone(&operation), Arc::clone(&scope_core));
        manager.register_settlement(
            Arc::clone(&scope_core),
            Arc::clone(&operation),
            Arc::clone(&lease),
            Arc::clone(&managed),
        );
        let execution = Arc::new(ExternalExecutionContext::new(
            Arc::clone(&operation),
            Arc::clone(&lease),
            Arc::clone(&scope_core),
            None,
        ));
        let execution_state = Arc::new(Mutex::new(Some(execution)));
        let managed_state = Arc::new(Mutex::new(Some(Arc::clone(&managed))));
        let adapter = Arc::new(SummaryGateAdapter {
            id,
            activations: AtomicUsize::new(0),
            stop_requests: AtomicUsize::new(0),
            abandon_calls: AtomicUsize::new(0),
            summary_ready: AtomicBool::new(true),
            summary_waker: Mutex::new(None),
            outcome_polls: AtomicUsize::new(0),
            summary_polls: AtomicUsize::new(0),
            release_polls: AtomicUsize::new(0),
            settlement_polls: AtomicUsize::new(0),
        });
        adapter.bind(id).unwrap();
        assert!(scope_core.request_stop(StopReason::UserCancelled));
        let published = Arc::new(Mutex::new(None));
        let publisher: ExternalOutcomePublisher<usize, ()> = Arc::new(Mutex::new(Some(Box::new({
            let published = Arc::clone(&published);
            move |outcome| *published.lock().unwrap() = Some(outcome)
        }))));
        let mut driver = Box::pin(drive_external(
            scope,
            Arc::clone(&adapter),
            Arc::new(Mutex::new(Some(Ok(())))),
            managed_state,
            execution_state,
            publisher,
            true,
        ));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(driver.as_mut().poll(&mut context).is_ready());
        assert_eq!(adapter.activations.load(Ordering::SeqCst), 1);
        assert_eq!(adapter.stop_requests.load(Ordering::SeqCst), 1);
        assert_eq!(adapter.outcome_polls.load(Ordering::SeqCst), 1);
        assert_eq!(adapter.release_polls.load(Ordering::SeqCst), 1);
        assert_eq!(adapter.settlement_polls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            published.lock().unwrap().take(),
            Some(OperationOutcome::Succeeded(7))
        ));
        scope_core.end();
        assert_eq!(lease_impl.execution_finished.load(Ordering::SeqCst), 1);
        assert_eq!(lease_impl.settlement_finished.load(Ordering::SeqCst), 1);
        manager.finish_shutdown();
    }

    #[test]
    fn test_external_driver_unpermitted_activation_abandons_and_settles() {
        let (manager, scope_core, scope, id) = manual_scope();
        let operation = OperationCore::new(id);
        scope_core.bind_operation_core(&operation);
        let lease_impl = Arc::new(LifecycleLease::default());
        let lease: Arc<dyn super::super::SubmissionLease> = lease_impl.clone();
        let managed = ManagedOperationControl::new(Arc::clone(&operation), Arc::clone(&scope_core));
        manager.register_settlement(
            Arc::clone(&scope_core),
            Arc::clone(&operation),
            Arc::clone(&lease),
            Arc::clone(&managed),
        );
        let execution = Arc::new(ExternalExecutionContext::new(
            Arc::clone(&operation),
            Arc::clone(&lease),
            Arc::clone(&scope_core),
            None,
        ));
        let execution_state = Arc::new(Mutex::new(Some(execution)));
        let managed_state = Arc::new(Mutex::new(Some(Arc::clone(&managed))));
        let adapter = Arc::new(SummaryGateAdapter {
            id,
            activations: AtomicUsize::new(0),
            stop_requests: AtomicUsize::new(0),
            abandon_calls: AtomicUsize::new(0),
            summary_ready: AtomicBool::new(true),
            summary_waker: Mutex::new(None),
            outcome_polls: AtomicUsize::new(0),
            summary_polls: AtomicUsize::new(0),
            release_polls: AtomicUsize::new(0),
            settlement_polls: AtomicUsize::new(0),
        });
        adapter.bind(id).unwrap();
        let published = Arc::new(Mutex::new(None));
        let publisher: ExternalOutcomePublisher<usize, ()> = Arc::new(Mutex::new(Some(Box::new({
            let published = Arc::clone(&published);
            move |outcome| *published.lock().unwrap() = Some(outcome)
        }))));
        let mut driver = Box::pin(drive_external(
            scope,
            Arc::clone(&adapter),
            Arc::new(Mutex::new(Some(Ok(())))),
            managed_state,
            execution_state,
            publisher,
            false,
        ));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(driver.as_mut().poll(&mut context).is_ready());
        assert_eq!(adapter.activations.load(Ordering::SeqCst), 0);
        assert_eq!(adapter.abandon_calls.load(Ordering::SeqCst), 1);
        assert_eq!(adapter.outcome_polls.load(Ordering::SeqCst), 0);
        assert_eq!(adapter.release_polls.load(Ordering::SeqCst), 1);
        assert_eq!(adapter.settlement_polls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            published.lock().unwrap().take(),
            Some(OperationOutcome::Stopped(crate::v02::report::StopReport {
                reason: StopReason::ParentStopped,
                ..
            }))
        ));
        scope_core.end();
        assert_eq!(lease_impl.execution_finished.load(Ordering::SeqCst), 1);
        assert_eq!(lease_impl.settlement_finished.load(Ordering::SeqCst), 1);
        manager.finish_shutdown();
    }
}

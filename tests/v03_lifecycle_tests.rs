// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Cross-protocol lifecycle tests using public crate-root APIs only.
use qubit_operation::ChildOutcomeSummary;
use qubit_operation::ChildRelease;
use qubit_operation::ChildSnapshot;
use qubit_operation::ExecutionFailure;
use qubit_operation::ExternalBindError;
use qubit_operation::OperationId;
use qubit_operation::OperationOutcome;
use qubit_operation::OperationScope;
use qubit_operation::PreparedExternalOperation;
use qubit_operation::SettlementReport;
use qubit_operation::StopReason;
use qubit_operation::StopReport;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct External {
    id: Mutex<Option<OperationId>>,
    phase: AtomicUsize,
    stop: Mutex<Option<StopReport>>,
    outcome_published: AtomicBool,
    release: AtomicBool,
    settlement: AtomicBool,
    release_waker: Mutex<Option<Waker>>,
    settlement_waker: Mutex<Option<Waker>>,
    settlement_polled: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    stop_forwarded: Mutex<Option<std::sync::mpsc::Sender<StopReason>>>,
    activated: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    typed_outcome_ready: AtomicBool,
    typed_outcome_waker: Mutex<Option<Waker>>,
    typed_poll_pending: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    worker_owner: Mutex<Option<Arc<DropProbe>>>,
    stops: AtomicUsize,
    polls: AtomicUsize,
}

impl External {
    fn held() -> Arc<Self> {
        Arc::new(Self {
            id: Mutex::new(None),
            phase: AtomicUsize::new(0),
            stop: Mutex::new(None),
            outcome_published: AtomicBool::new(false),
            release: AtomicBool::new(false),
            settlement: AtomicBool::new(false),
            release_waker: Mutex::new(None),
            settlement_waker: Mutex::new(None),
            settlement_polled: Mutex::new(None),
            stop_forwarded: Mutex::new(None),
            activated: Mutex::new(None),
            typed_outcome_ready: AtomicBool::new(true),
            typed_outcome_waker: Mutex::new(None),
            typed_poll_pending: Mutex::new(None),
            worker_owner: Mutex::new(None),
            stops: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
        })
    }
    fn open(flag: &AtomicBool, waker: &Mutex<Option<Waker>>) {
        flag.store(true, Ordering::SeqCst);
        if let Some(waker) = waker.lock().unwrap().take() {
            waker.wake();
        }
    }
    fn release_input(&self) {
        self.worker_owner.lock().unwrap().take();
        Self::open(&self.release, &self.release_waker);
        self.phase.store(2, Ordering::SeqCst);
    }
    fn release_settlement(&self) {
        Self::open(&self.settlement, &self.settlement_waker);
    }
    fn release_typed_outcome(&self) {
        self.typed_outcome_ready.store(true, Ordering::SeqCst);
        if let Some(waker) = self.typed_outcome_waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

impl OperationControl for External {
    fn request_stop(&self, reason: StopReason) {
        self.stops.fetch_add(1, Ordering::SeqCst);
        let mut stop = self.stop.lock().unwrap();
        if stop.is_none() {
            *stop = Some(StopReport {
                reason,
                accepted_at: None,
            });
        }
        if let Some(signal) = self.stop_forwarded.lock().unwrap().take() {
            let _ = signal.send(reason);
        }
    }
    fn snapshot(&self) -> ChildSnapshot {
        let operation_id = self.id.lock().unwrap().expect("bound");
        let outcome = self
            .outcome_published
            .load(Ordering::SeqCst)
            .then(|| ChildOutcomeSummary {
                operation_id,
                kind: ChildOutcomeKind::Succeeded,
                summary: DiagnosticSummary::new("worker completed successfully"),
            });
        ChildSnapshot {
            operation_id,
            execution: match self.phase.load(Ordering::SeqCst) {
                0 => ExecutionPhase::Queued,
                1 => ExecutionPhase::Running,
                _ => ExecutionPhase::Finished,
            },
            stop: self.stop.lock().unwrap().clone(),
            outcome,
            input_released: self.release.load(Ordering::SeqCst),
            settlement: None,
        }
    }
    fn poll_outcome(&self, _: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
        Poll::Ready(ChildOutcomeSummary {
            operation_id: self.id.lock().unwrap().expect("bound"),
            kind: ChildOutcomeKind::Succeeded,
            summary: DiagnosticSummary::new("worker completed successfully"),
        })
    }
    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        if self.release.load(Ordering::SeqCst) {
            Poll::Ready(ChildRelease {
                operation_id: self.id.lock().unwrap().expect("bound"),
                confirmed_at: None,
            })
        } else {
            *self.release_waker.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(signal) = self.settlement_polled.lock().unwrap().take() {
            let _ = signal.send(());
        }
        if !self.settlement.load(Ordering::SeqCst) {
            *self.settlement_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(Arc::new(SettlementReport {
            operation_id: self.id.lock().unwrap().expect("bound"),
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
        }))
    }
}
impl PreparedExternalOperation<usize, ()> for External {
    fn bind(&self, id: OperationId) -> Result<(), ExternalBindError> {
        let mut slot = self.id.lock().unwrap();
        if slot.is_some() {
            return Err(ExternalBindError::AlreadyBound);
        }
        *slot = Some(id);
        Ok(())
    }
    fn activate(self: Arc<Self>, _: OperationScope) -> Result<(), ExecutionFailure> {
        self.phase.store(1, Ordering::SeqCst);
        if let Some(signal) = self.activated.lock().unwrap().take() {
            let _ = signal.send(());
        }
        Ok(())
    }
    fn abandon(self: Arc<Self>) {}
    fn poll_take_outcome(&self, cx: &mut Context<'_>) -> Poll<OperationOutcome<usize, ()>> {
        let mut waker = self.typed_outcome_waker.lock().unwrap();
        if !self.typed_outcome_ready.load(Ordering::SeqCst) {
            *waker = Some(cx.waker().clone());
            if let Some(signal) = self.typed_poll_pending.lock().unwrap().take() {
                let _ = signal.send(());
            }
            return Poll::Pending;
        }
        drop(waker);
        self.outcome_published.store(true, Ordering::SeqCst);
        self.phase.store(2, Ordering::SeqCst);
        Poll::Ready(OperationOutcome::Succeeded(42))
    }
}

#[test]
fn test_stopped_early_outcome_waits_for_input_release_and_domain_settlement() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let external = External::held();
    let owner_drops = Arc::new(AtomicUsize::new(0));
    *external.worker_owner.lock().unwrap() = Some(Arc::new(DropProbe(Arc::clone(&owner_drops))));
    external.typed_outcome_ready.store(false, Ordering::SeqCst);
    let (typed_pending_tx, typed_pending_rx) = std::sync::mpsc::channel();
    *external.typed_poll_pending.lock().unwrap() = Some(typed_pending_tx);
    let (activated_tx, activated_rx) = std::sync::mpsc::channel();
    *external.activated.lock().unwrap() = Some(activated_tx);
    let (stopped_tx, stopped_rx) = std::sync::mpsc::channel();
    *external.stop_forwarded.lock().unwrap() = Some(stopped_tx);
    let (polled_tx, polled_rx) = std::sync::mpsc::channel();
    *external.settlement_polled.lock().unwrap() = Some(polled_tx);
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&external))
        .unwrap();
    activated_rx.recv().unwrap();
    typed_pending_rx.recv().unwrap();
    assert_eq!(
        handle.request_stop(StopReason::UserCancelled),
        StopRequestStatus::Accepted
    );
    assert_eq!(stopped_rx.recv().unwrap(), StopReason::UserCancelled);
    external.release_typed_outcome();
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::Stopped(_)
    ));
    let domain_summary = match external.poll_outcome(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(summary) => summary,
        Poll::Pending => panic!("domain completion summary is ready"),
    };
    let domain_snapshot = external.snapshot();
    assert_eq!(domain_summary.kind, ChildOutcomeKind::Succeeded);
    assert_eq!(domain_snapshot.outcome.as_ref(), Some(&domain_summary));
    assert_eq!(domain_snapshot.execution, ExecutionPhase::Finished);
    assert!(!domain_snapshot.input_released);
    assert_eq!(
        domain_snapshot.stop.as_ref().map(|stop| stop.reason),
        Some(StopReason::UserCancelled)
    );
    assert_eq!(
        domain_summary.kind,
        ChildOutcomeKind::Succeeded,
        "domain reports success consistently; framework stop policy publishes Stopped"
    );
    polled_rx.recv().unwrap();
    assert!(matches!(
        handle
            .wait_settlement_for(std::time::Duration::ZERO)
            .unwrap(),
        SettlementWait::Pending(_)
    ));
    assert_eq!(external.stops.load(Ordering::SeqCst), 1);
    assert_eq!(
        owner_drops.load(Ordering::SeqCst),
        0,
        "temp owner remains with the running worker after early Stopped"
    );
    assert!(external.polls.load(Ordering::SeqCst) > 0);
    external.release_input();
    assert_eq!(
        owner_drops.load(Ordering::SeqCst),
        1,
        "worker relinquishes the temp only at release"
    );
    external.release_settlement();
    assert!(handle.wait_settlement().unwrap().all_released());
}

#[test]
fn test_queue_capacity_does_not_block_stop_for_an_accepted_external_operation() {
    use std::sync::mpsc;
    let mut options = ManagerOptions::default();
    options.sync_executor = ThreadPoolExecutorOptions {
        worker_count: 1,
        queue_capacity: 1,
    };
    let manager = OperationManager::new(options).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut running = manager
        .submit(OperationOptions::default(), move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    started_rx.recv().unwrap();
    let external = External::held();
    let (stopped_tx, stopped_rx) = mpsc::channel();
    *external.stop_forwarded.lock().unwrap() = Some(stopped_tx);
    let mut queued = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&external))
        .unwrap();

    let (body_started_tx, body_started_rx) = mpsc::channel();
    let rejected = match manager.try_submit(OperationOptions::default(), move |_| {
        let _ = body_started_tx.send(());
        Ok::<_, ()>(())
    }) {
        Err(error) => error,
        Ok(_) => panic!("second submission must be rejected while the only queue slot is occupied"),
    };
    let (reason, returned_body) = rejected.into_parts();
    assert_eq!(reason, AdmissionError::Busy);
    drop(returned_body);
    assert!(
        body_started_rx.try_recv().is_err(),
        "rejected body is returned without execution"
    );

    assert_eq!(
        queued.request_stop(StopReason::UserCancelled),
        StopRequestStatus::Accepted
    );
    release_tx.send(()).unwrap();
    assert_eq!(stopped_rx.recv().unwrap(), StopReason::UserCancelled);
    assert!(matches!(
        queued.wait_outcome().unwrap(),
        OperationOutcome::Stopped(_)
    ));
    external.release_input();
    external.release_settlement();
    queued.wait_settlement().unwrap();
    let _ = running.wait_outcome().unwrap();
    running.wait_settlement().unwrap();
}

struct DropProbe(Arc<AtomicUsize>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct ResourceExternal {
    control: Arc<External>,
    result: Mutex<Option<ResourceRef<DropProbe>>>,
}
impl OperationControl for ResourceExternal {
    fn request_stop(&self, reason: StopReason) {
        self.control.request_stop(reason);
    }
    fn snapshot(&self) -> ChildSnapshot {
        self.control.snapshot()
    }
    fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
        self.control.poll_outcome(cx)
    }
    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        self.control.poll_release(cx)
    }
    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>> {
        self.control.poll_settlement(cx)
    }
}
impl PreparedExternalOperation<ResourceRef<DropProbe>, ()> for ResourceExternal {
    fn bind(&self, id: OperationId) -> Result<(), ExternalBindError> {
        self.control.bind(id)
    }
    fn activate(self: Arc<Self>, scope: OperationScope) -> Result<(), ExecutionFailure> {
        *self.result.lock().unwrap() = Some(
            scope
                .manage(DropProbe(Arc::new(AtomicUsize::new(0))))
                .expect("resource reservation"),
        );
        Ok(())
    }
    fn abandon(self: Arc<Self>) {}
    fn poll_take_outcome(
        &self,
        _: &mut Context<'_>,
    ) -> Poll<OperationOutcome<ResourceRef<DropProbe>, ()>> {
        Poll::Ready(OperationOutcome::Succeeded(
            self.result.lock().unwrap().take().expect("activated"),
        ))
    }
}

#[test]
fn test_resource_valued_typed_outcome_must_be_dropped_before_settlement() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let control = External::held();
    let (polled_tx, polled_rx) = std::sync::mpsc::channel();
    *control.settlement_polled.lock().unwrap() = Some(polled_tx);
    let external = Arc::new(ResourceExternal {
        control,
        result: Mutex::new(None),
    });
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&external))
        .unwrap();
    let outcome = handle.wait_outcome().unwrap();
    polled_rx.recv().unwrap();
    assert!(matches!(
        handle
            .wait_settlement_for(std::time::Duration::ZERO)
            .unwrap(),
        SettlementWait::Pending(_)
    ));
    external.control.release_input();
    external.control.release_settlement();
    assert!(
        matches!(
            handle
                .wait_settlement_for(std::time::Duration::ZERO)
                .unwrap(),
            SettlementWait::Pending(_)
        ),
        "the returned ResourceRef keeps framework settlement pending"
    );
    drop(outcome);
    assert!(handle.wait_settlement().unwrap().all_released());
}

struct FailingTempCleanup(Arc<Mutex<Option<ResourceId>>>);
impl AsyncCleanup<DropProbe> for FailingTempCleanup {
    fn cleanup(
        self: Arc<Self>,
        resource: DropProbe,
        _: CleanupContext,
    ) -> CleanupFuture<DropProbe> {
        Box::pin(std::future::ready(CleanupOutcome::Failed {
            failure: CleanupFailure {
                resource_id: self.0.lock().unwrap().expect("resource id set"),
                stage: CleanupStage::AsyncRelease,
                summary: DiagnosticSummary::new("temp still mounted"),
                source: None,
            },
            resource,
        }))
    }
}

#[test]
fn test_failed_temp_cleanup_leaves_an_owner_in_the_recovery_store() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let id_slot = Arc::new(Mutex::new(None));
    let id_for_body = Arc::clone(&id_slot);
    let drops = Arc::new(AtomicUsize::new(0));
    let drops_for_body = Arc::clone(&drops);
    let completion = manager
        .run_and_settle(OperationOptions::default(), move |scope| {
            let resources = scope
                .resource_scope(ResourceScopeOptions { capacity: 1 })
                .unwrap();
            let owner = resources
                .manage_with(
                    DropProbe(drops_for_body),
                    Arc::new(FailingTempCleanup(Arc::clone(&id_for_body))),
                )
                .unwrap();
            *id_for_body.lock().unwrap() = Some(owner.id());
            drop(owner);
            Ok::<_, ()>(())
        })
        .unwrap();
    assert_eq!(completion.settlement.recovery_count, 1);
    let recovery = completion.settlement.recoveries[0].clone();
    let id = id_slot.lock().unwrap().expect("cleanup records id");
    assert_eq!(recovery.snapshot().resources.as_ref(), [id]);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "failed cleanup returns the live owner to recovery"
    );
}

#[test]
fn test_shutdown_timeout_keeps_external_owner_observable_until_release() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let external = External::held();
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&external))
        .unwrap();
    let shutdown = manager.request_shutdown(ShutdownMode::Cancel);
    assert!(matches!(
        shutdown.wait_for(std::time::Duration::ZERO).unwrap(),
        ShutdownWait::Pending(_)
    ));
    assert!(matches!(
        handle
            .wait_settlement_for(std::time::Duration::ZERO)
            .unwrap(),
        SettlementWait::Pending(_)
    ));
    external.release_input();
    external.release_settlement();
    let _ = handle.wait_outcome().unwrap();
    handle.wait_settlement().unwrap();
    let _report = shutdown.wait().unwrap();
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use qubit_operation::AdmissionError;
use qubit_operation::AsyncOperationManager;
use qubit_operation::ChildOutcomeKind;
use qubit_operation::ChildOutcomeSummary;
use qubit_operation::ChildRelease;
use qubit_operation::ChildSnapshot;
use qubit_operation::DiagnosticSummary;
use qubit_operation::EffectFact;
use qubit_operation::EffectRecord;
use qubit_operation::ExecutionFailure;
use qubit_operation::ExecutionPhase;
use qubit_operation::ExternalBindError;
use qubit_operation::ExternalProtocolFaultKind;
use qubit_operation::ExternalProtocolPort;
use qubit_operation::ManagerLimits;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationControl;
use qubit_operation::OperationId;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::OperationScope;
use qubit_operation::PreparedExternalOperation;
use qubit_operation::SettlementReport;
use qubit_operation::SettlementStatus;
use qubit_operation::SettlementWait;
use qubit_operation::ShutdownMode;
use qubit_operation::StopReason;
use qubit_operation::StopRequestStatus;
use qubit_operation::ThreadPoolExecutorOptions;

struct DropPanicsOnDrop(Arc<std::sync::atomic::AtomicUsize>);

const PANIC_PAYLOAD_SNAPSHOT: usize = 1 << 0;
const PANIC_PAYLOAD_ACTIVATE: usize = 1 << 1;
const PANIC_PAYLOAD_TAKE_OUTCOME: usize = 1 << 2;
const PANIC_PAYLOAD_SUMMARY: usize = 1 << 3;
const PANIC_PAYLOAD_STOP: usize = 1 << 4;
const PANIC_PAYLOAD_ABANDON: usize = 1 << 5;
const PANIC_PAYLOAD_RELEASE: usize = 1 << 6;
const PANIC_PAYLOAD_SETTLEMENT: usize = 1 << 7;

impl Drop for DropPanicsOnDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        panic!("payload drop probe");
    }
}

struct Prepared {
    activated: std::sync::atomic::AtomicUsize,
    id: std::sync::Mutex<Option<OperationId>>,
    release_ready: std::sync::atomic::AtomicBool,
    release_waker: std::sync::Mutex<Option<std::task::Waker>>,
    release_polls: std::sync::atomic::AtomicUsize,
    fail_bind: std::sync::atomic::AtomicBool,
    panic_bind_payload: std::sync::atomic::AtomicBool,
    panic_payload_flags: std::sync::atomic::AtomicUsize,
    payload_drop_attempts: Arc<std::sync::atomic::AtomicUsize>,
    abandon_calls: std::sync::atomic::AtomicUsize,
    activation_thread: std::sync::Mutex<Option<String>>,
    bind_gate: std::sync::Mutex<Option<BindGate>>,
    stop_calls: std::sync::atomic::AtomicUsize,
    outcome_ready: std::sync::atomic::AtomicBool,
    outcome_waker: std::sync::Mutex<Option<Waker>>,
    outcome_polls: std::sync::atomic::AtomicUsize,
    panic_snapshot: std::sync::atomic::AtomicBool,
    panic_activate: std::sync::atomic::AtomicBool,
    panic_flags: std::sync::atomic::AtomicUsize,
    summary_ready: std::sync::atomic::AtomicBool,
    summary_waker: std::sync::Mutex<Option<Waker>>,
    summary_polls: std::sync::atomic::AtomicUsize,
    summary_kind: std::sync::atomic::AtomicUsize,
    summary_id: std::sync::Mutex<Option<OperationId>>,
    protocol_fault_id: std::sync::Mutex<Option<OperationId>>,
    typed_outcome: std::sync::Mutex<Option<OperationOutcome<usize, ()>>>,
    settlement_ready: std::sync::atomic::AtomicBool,
    settlement_waker: std::sync::Mutex<Option<Waker>>,
    settlement_polls: std::sync::atomic::AtomicUsize,
    domain_settlement_status: std::sync::Mutex<Option<SettlementStatus>>,
    captured_manager: std::sync::Mutex<Option<OperationManager>>,
    child_started_tx: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    child_accepted_tx: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    driver_waiting_tx: std::sync::Mutex<Option<std::sync::mpsc::Sender<()>>>,
    child_handle: std::sync::Mutex<Option<qubit_operation::OperationHandle<usize, ()>>>,
}

struct BindGate {
    entered: std::sync::mpsc::Sender<()>,
    proceed: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

impl OperationControl for Prepared {
    fn request_stop(&self, _: StopReason) {
        self.panic_if_requested(PANIC_PAYLOAD_STOP);
        assert_eq!(
            self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 1,
            0,
            "request_stop panic requested"
        );
        self.stop_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn snapshot(&self) -> ChildSnapshot {
        self.panic_if_requested(PANIC_PAYLOAD_SNAPSHOT);
        assert!(
            !self.panic_snapshot.load(std::sync::atomic::Ordering::SeqCst),
            "snapshot panic requested"
        );
        ChildSnapshot {
            operation_id: self.id.lock().unwrap().expect("bound before polling"),
            execution: ExecutionPhase::Queued,
            stop: None,
            outcome: None,
            input_released: false,
            settlement: None,
        }
    }
    fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
        self.panic_if_requested(PANIC_PAYLOAD_SUMMARY);
        assert_eq!(
            self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 2,
            0,
            "poll_outcome panic requested"
        );
        self.summary_polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !self.summary_ready.load(std::sync::atomic::Ordering::SeqCst) {
            *self.summary_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let operation_id = self
            .summary_id
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| self.id.lock().unwrap().expect("bound before polling"));
        let (kind, summary) = match self.summary_kind.load(std::sync::atomic::Ordering::SeqCst) {
            1 => (ChildOutcomeKind::Failed, "failed for a domain-specific reason"),
            2 => (ChildOutcomeKind::Stopped, "stopped by a policy decision"),
            3 => (ChildOutcomeKind::Failed, "failure classification mismatch"),
            _ => (ChildOutcomeKind::Succeeded, "inference completed"),
        };
        Poll::Ready(ChildOutcomeSummary {
            operation_id,
            kind,
            summary: DiagnosticSummary::new(summary),
        })
    }
    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        self.panic_if_requested(PANIC_PAYLOAD_RELEASE);
        assert_eq!(
            self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 4,
            0,
            "poll_release panic requested"
        );
        self.release_polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !self.release_ready.load(std::sync::atomic::Ordering::SeqCst) {
            *self.release_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(ChildRelease {
            operation_id: if self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 64 != 0 {
                self.protocol_fault_id.lock().unwrap().expect("fault ID set")
            } else {
                self.id.lock().unwrap().expect("bound before polling")
            },
            confirmed_at: None,
        })
    }
    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>> {
        self.panic_if_requested(PANIC_PAYLOAD_SETTLEMENT);
        assert_eq!(
            self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 8,
            0,
            "poll_settlement panic requested"
        );
        self.settlement_polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !self.settlement_ready.load(std::sync::atomic::Ordering::SeqCst) {
            *self.settlement_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let operation_id = if self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 128 != 0 {
            self.protocol_fault_id.lock().unwrap().expect("fault ID set")
        } else {
            self.id.lock().unwrap().expect("bound before polling")
        };
        let status = self
            .domain_settlement_status
            .lock()
            .unwrap()
            .unwrap_or(SettlementStatus::Released);
        let recovery = status == SettlementStatus::RecoveryRequired;
        let transferred = status == SettlementStatus::Transferred;
        Poll::Ready(Arc::new(SettlementReport {
            operation_id,
            status,
            released_count: 0,
            transferred_count: usize::from(transferred),
            recovery_count: usize::from(recovery),
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
            transfers_total: usize::from(transferred) + usize::from(transferred),
            transfers_truncated: transferred,
            recoveries: Arc::from([]),
            recoveries_total: usize::from(recovery) + usize::from(recovery),
            recoveries_truncated: recovery,
            effects: Arc::from([EffectRecord {
                operation_id,
                resource_id: None,
                fact: EffectFact::Known,
                description: DiagnosticSummary::new("external side effect"),
            }]),
            effects_total: 2,
            effect_overflow: 1,
            external_settlement: None,
        }))
    }
}

impl Prepared {
    fn panic_if_requested(&self, flag: usize) {
        if self.panic_payload_flags.load(std::sync::atomic::Ordering::SeqCst) & flag != 0 {
            std::panic::panic_any(DropPanicsOnDrop(Arc::clone(&self.payload_drop_attempts)));
        }
    }

    fn release_settlement(&self) {
        self.settlement_ready.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = self.settlement_waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn release_summary(&self) {
        self.summary_ready.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = self.summary_waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn ready_fixture() -> Arc<Self> {
        Arc::new(Self {
            activated: std::sync::atomic::AtomicUsize::new(0),
            id: std::sync::Mutex::new(None),
            release_ready: std::sync::atomic::AtomicBool::new(true),
            release_waker: std::sync::Mutex::new(None),
            release_polls: std::sync::atomic::AtomicUsize::new(0),
            fail_bind: std::sync::atomic::AtomicBool::new(false),
            panic_bind_payload: std::sync::atomic::AtomicBool::new(false),
            panic_payload_flags: std::sync::atomic::AtomicUsize::new(0),
            payload_drop_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            abandon_calls: std::sync::atomic::AtomicUsize::new(0),
            activation_thread: std::sync::Mutex::new(None),
            bind_gate: std::sync::Mutex::new(None),
            stop_calls: std::sync::atomic::AtomicUsize::new(0),
            outcome_ready: std::sync::atomic::AtomicBool::new(true),
            outcome_waker: std::sync::Mutex::new(None),
            outcome_polls: std::sync::atomic::AtomicUsize::new(0),
            panic_snapshot: std::sync::atomic::AtomicBool::new(false),
            panic_activate: std::sync::atomic::AtomicBool::new(false),
            panic_flags: std::sync::atomic::AtomicUsize::new(0),
            summary_ready: std::sync::atomic::AtomicBool::new(true),
            summary_waker: std::sync::Mutex::new(None),
            summary_polls: std::sync::atomic::AtomicUsize::new(0),
            summary_kind: std::sync::atomic::AtomicUsize::new(0),
            summary_id: std::sync::Mutex::new(None),
            protocol_fault_id: std::sync::Mutex::new(None),
            typed_outcome: std::sync::Mutex::new(None),
            settlement_ready: std::sync::atomic::AtomicBool::new(true),
            settlement_waker: std::sync::Mutex::new(None),
            settlement_polls: std::sync::atomic::AtomicUsize::new(0),
            domain_settlement_status: std::sync::Mutex::new(None),
            captured_manager: std::sync::Mutex::new(None),
            child_started_tx: std::sync::Mutex::new(None),
            child_accepted_tx: std::sync::Mutex::new(None),
            driver_waiting_tx: std::sync::Mutex::new(None),
            child_handle: std::sync::Mutex::new(None),
        })
    }

    fn release_outcome(&self) {
        self.outcome_ready.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = self.outcome_waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn release_inputs(&self) {
        self.release_ready.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = self.release_waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

#[test]
fn test_external_payload_bind_drop_panic_does_not_escape() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Prepared::ready_fixture();
    prepared
        .panic_bind_payload
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let submitted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        manager.try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
    }));
    assert!(submitted.is_ok(), "secondary panic escaped external bind handling");
    let mut external = submitted
        .unwrap()
        .expect("bind panic should remain an accepted setup failure");
    assert_eq!(
        prepared.payload_drop_attempts.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert!(matches!(
        external.wait_outcome().unwrap(),
        OperationOutcome::ExecutionFailed(_)
    ));
    external.wait_settlement().unwrap();
    let mut ordinary = manager
        .try_submit(OperationOptions::default(), |_| Ok::<u8, ()>(1))
        .unwrap();
    assert!(matches!(
        ordinary.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(1)
    ));
    ordinary.wait_settlement().unwrap();
}

#[test]
fn test_external_payload_async_bind_drop_panic_does_not_escape() {
    let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Prepared::ready_fixture();
    prepared
        .panic_bind_payload
        .store(true, std::sync::atomic::Ordering::SeqCst);

    let submitted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        manager.try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
    }));
    assert!(
        submitted.is_ok(),
        "secondary panic escaped async external bind handling"
    );
    let mut external = submitted
        .unwrap()
        .expect("bind panic should remain an accepted setup failure");
    assert_eq!(
        prepared.payload_drop_attempts.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert!(matches!(
        block_on_bounded(external.wait_outcome(), "async external outcome", || format!(
            "outcome_polls={}",
            prepared.outcome_polls.load(std::sync::atomic::Ordering::SeqCst)
        ))
        .unwrap(),
        OperationOutcome::ExecutionFailed(_)
    ));
    let _ = block_on_bounded(external.wait_settlement(), "async external settlement", || {
        format!(
            "settlement_polls={}",
            prepared.settlement_polls.load(std::sync::atomic::Ordering::SeqCst)
        )
    });
}

#[test]
fn test_external_payload_driver_callback_matrix_preserves_progress() {
    for (name, flag, fail_bind, expected_activated, expected_abandoned) in [
        ("snapshot", PANIC_PAYLOAD_SNAPSHOT, false, 0, 1),
        ("activate", PANIC_PAYLOAD_ACTIVATE, false, 1, 0),
        ("take_outcome", PANIC_PAYLOAD_TAKE_OUTCOME, false, 1, 0),
        ("summary", PANIC_PAYLOAD_SUMMARY, false, 1, 0),
        ("abandon", PANIC_PAYLOAD_ABANDON, true, 0, 1),
    ] {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let prepared = Prepared::ready_fixture();
        prepared
            .panic_payload_flags
            .store(flag, std::sync::atomic::Ordering::SeqCst);
        prepared.fail_bind.store(fail_bind, std::sync::atomic::Ordering::SeqCst);
        let handle = manager
            .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
            .unwrap_or_else(|_| panic!("{name} payload fixture should be admitted"));

        let (outcome, settlement) = wait_handle_bounded(handle);
        assert!(
            matches!(outcome, OperationOutcome::ExecutionFailed(_)),
            "{name} keeps its existing failure mapping"
        );
        assert_eq!(
            settlement.status,
            SettlementStatus::Released,
            "{name} preserves provider release evidence"
        );
        assert_eq!(
            prepared.payload_drop_attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "{name} payload is consumed once"
        );
        assert_eq!(
            prepared.activated.load(std::sync::atomic::Ordering::SeqCst),
            expected_activated,
            "{name} activation strategy"
        );
        assert_eq!(
            prepared.abandon_calls.load(std::sync::atomic::Ordering::SeqCst),
            expected_abandoned,
            "{name} abandon strategy"
        );

        let ordinary = manager
            .try_submit(OperationOptions::default(), |_| Ok::<u8, ()>(1))
            .unwrap();
        let (outcome, settlement) = wait_handle_bounded(ordinary);
        assert!(
            matches!(outcome, OperationOutcome::Succeeded(1)),
            "ordinary operation progresses after {name}"
        );
        assert_eq!(settlement.status, SettlementStatus::Released);
    }
}

#[test]
fn test_external_protocol_fault_panics_are_queryable_and_stop_polling() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let seed = Prepared::ready_fixture();
    let mut seed_handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&seed))
        .unwrap();
    let foreign_id = seed.id.lock().unwrap().expect("seed operation is bound");
    let _ = seed_handle.wait_outcome().unwrap();
    seed_handle.wait_settlement().unwrap();
    for (mask, port, mismatch) in [
        (4, ExternalProtocolPort::Release, false),
        (8, ExternalProtocolPort::Settlement, false),
        (0, ExternalProtocolPort::Release, true),
        (0, ExternalProtocolPort::Settlement, true),
    ] {
        let prepared = Prepared::ready_fixture();
        prepared.panic_flags.store(mask, std::sync::atomic::Ordering::SeqCst);
        if mismatch {
            *prepared.protocol_fault_id.lock().unwrap() = Some(foreign_id);
            prepared.panic_flags.store(
                if port == ExternalProtocolPort::Release { 64 } else { 128 },
                std::sync::atomic::Ordering::SeqCst,
            );
        }
        let mut handle = manager
            .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
            .unwrap();
        assert!(matches!(handle.wait_outcome().unwrap(), OperationOutcome::Succeeded(7)));
        let observer = handle.observer();
        let faults = observer.external_protocol_faults();
        assert_eq!(faults.len(), 1);
        assert_eq!(faults[0].operation_id, handle.id());
        assert_eq!(faults[0].port, port);
        assert_eq!(
            faults[0].kind,
            if mismatch {
                ExternalProtocolFaultKind::IdentityMismatch
            } else {
                ExternalProtocolFaultKind::Panicked
            }
        );
        assert_eq!(faults[0].observed_operation_id, mismatch.then_some(foreign_id));
        assert_eq!(handle.external_protocol_faults().as_ref(), faults.as_ref());
        assert_eq!(observer.as_async().external_protocol_faults().as_ref(), faults.as_ref());
        let polls = match port {
            ExternalProtocolPort::Release => prepared.release_polls.load(std::sync::atomic::Ordering::SeqCst),
            ExternalProtocolPort::Settlement => prepared.settlement_polls.load(std::sync::atomic::Ordering::SeqCst),
        };
        std::thread::sleep(std::time::Duration::from_millis(2));
        let later_polls = match port {
            ExternalProtocolPort::Release => prepared.release_polls.load(std::sync::atomic::Ordering::SeqCst),
            ExternalProtocolPort::Settlement => prepared.settlement_polls.load(std::sync::atomic::Ordering::SeqCst),
        };
        assert_eq!(polls, later_polls, "faulted port must stop polling");
        assert!(matches!(
            handle.wait_settlement_for(std::time::Duration::ZERO).unwrap(),
            SettlementWait::Pending(_)
        ));
        let async_handle = handle.into_async();
        assert_eq!(async_handle.external_protocol_faults().as_ref(), faults.as_ref());
        assert_eq!(
            async_handle.observer().as_sync().external_protocol_faults().as_ref(),
            faults.as_ref()
        );
    }
}

fn wait_handle_bounded<T: Send + 'static, E: Send + 'static>(
    mut handle: qubit_operation::OperationHandle<T, E>,
) -> (OperationOutcome<T, E>, Arc<SettlementReport>) {
    let observer = handle.observer();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let outcome = handle.wait_outcome();
        let settlement = handle.wait_settlement();
        let _ = sender.send((outcome, settlement));
    });
    let (outcome, settlement) = recv_bounded(&receiver, "operation outcome and settlement", move || {
        format!("{:?}", observer.snapshot())
    });
    (
        outcome.expect("operation outcome observation"),
        settlement.expect("operation settlement"),
    )
}

fn wait_outcome_bounded<T: Send + 'static, E: Send + 'static>(
    mut handle: qubit_operation::OperationHandle<T, E>,
) -> OperationOutcome<T, E> {
    let observer = handle.observer();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _ = sender.send(handle.wait_outcome());
    });
    recv_bounded(&receiver, "operation outcome", move || {
        format!("{:?}", observer.snapshot())
    })
    .expect("operation outcome observation")
}

fn wait_outcome_return_handle_bounded<T: Send + 'static, E: Send + 'static>(
    mut handle: qubit_operation::OperationHandle<T, E>,
    stage: &'static str,
) -> (qubit_operation::OperationHandle<T, E>, OperationOutcome<T, E>) {
    let observer = handle.observer();
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let outcome = handle.wait_outcome();
        let _ = sender.send((handle, outcome));
    });
    let (handle, outcome) = recv_bounded(&receiver, stage, move || format!("{:?}", observer.snapshot()));
    (handle, outcome.expect("operation outcome observation"))
}

fn wait_for_counter(stage: &str, count: impl Fn() -> usize, snapshot: impl Fn() -> String) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if count() > 0 {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "watchdog expired waiting for {stage}; count={}; snapshot={}",
                count(),
                snapshot()
            );
        }
        std::thread::yield_now();
    }
}

fn wait_future_bounded<F: Future>(
    mut future: std::pin::Pin<&mut F>,
    context: &mut Context<'_>,
    stage: &str,
    snapshot: impl Fn() -> String,
) -> F::Output {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(context) {
            return output;
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            panic!("watchdog expired waiting for {stage}; snapshot={}", snapshot());
        }
        std::thread::park_timeout(deadline.saturating_duration_since(now));
    }
}

fn recv_bounded<T>(receiver: &std::sync::mpsc::Receiver<T>, stage: &str, snapshot: impl Fn() -> String) -> T {
    match receiver.recv_timeout(std::time::Duration::from_secs(5)) {
        Ok(value) => value,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("watchdog expired waiting for {stage}; snapshot={}", snapshot());
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!(
                "channel disconnected while waiting for {stage}; snapshot={}",
                snapshot()
            );
        }
    }
}

fn recv_optional<T>(
    receiver: &std::sync::mpsc::Receiver<T>,
    timeout: std::time::Duration,
    stage: &str,
    snapshot: impl Fn() -> String,
) -> Option<T> {
    match receiver.recv_timeout(timeout) {
        Ok(value) => Some(value),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("channel disconnected while observing {stage}; snapshot={}", snapshot());
        }
    }
}

fn join_bounded<T: Send + 'static>(
    worker: std::thread::JoinHandle<T>,
    stage: &str,
    snapshot: impl Fn() -> String + Send + 'static,
) -> T {
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let _ = sender.send(worker.join());
    });
    recv_bounded(&receiver, stage, snapshot).unwrap_or_else(|_| panic!("worker panicked during {stage}"))
}

impl PreparedExternalOperation<usize, ()> for Prepared {
    fn bind(&self, id: OperationId) -> Result<(), ExternalBindError> {
        let mut bound = self.id.lock().unwrap();
        if bound.is_some() {
            return Err(ExternalBindError::AlreadyBound);
        }
        *bound = Some(id);
        drop(bound);
        if let Some(gate) = self.bind_gate.lock().unwrap().take() {
            let _ = gate.entered.send(());
            let (ready, changed) = &*gate.proceed;
            let mut ready = ready.lock().unwrap();
            while !*ready {
                ready = changed.wait(ready).unwrap();
            }
        }
        if self.panic_bind_payload.load(std::sync::atomic::Ordering::SeqCst) {
            std::panic::panic_any(DropPanicsOnDrop(Arc::clone(&self.payload_drop_attempts)));
        }
        if self.fail_bind.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(ExternalBindError::Failed(ExecutionFailure {
                operation_id: id,
                summary: DiagnosticSummary::new("bind failed after claim"),
                source: None,
            }));
        }
        Ok(())
    }
    fn activate(self: Arc<Self>, scope: OperationScope) -> Result<(), ExecutionFailure> {
        self.activated.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.panic_if_requested(PANIC_PAYLOAD_ACTIVATE);
        assert!(
            !self.panic_activate.load(std::sync::atomic::Ordering::SeqCst),
            "activate panic requested"
        );
        if self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 32 != 0 {
            return Err(ExecutionFailure {
                operation_id: self.id.lock().unwrap().expect("bound before activation"),
                summary: DiagnosticSummary::new("activate returned an error"),
                source: None,
            });
        }
        *self.activation_thread.lock().unwrap() = std::thread::current().name().map(str::to_owned);
        *self.captured_manager.lock().unwrap() = Some(scope.manager());
        if self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 256 != 0 {
            let _resource = scope
                .manage(DropPanicsOnDrop(Arc::clone(&self.payload_drop_attempts)))
                .expect("test recovery resource should be admitted");
        }
        if let Some(started) = self.child_started_tx.lock().unwrap().take() {
            let parent = Arc::clone(&self);
            let child = scope
                .manager()
                .try_submit(OperationOptions::default(), move |_| {
                    let _ = started.send(());
                    parent.release_outcome();
                    parent.release_inputs();
                    parent.release_summary();
                    parent.release_settlement();
                    Ok::<usize, ()>(41)
                })
                .map_err(|_| ExecutionFailure {
                    operation_id: self.id.lock().unwrap().expect("bound before activation"),
                    summary: DiagnosticSummary::new("probe child admission failed"),
                    source: None,
                })?;
            *self.child_handle.lock().unwrap() = Some(child);
            if let Some(accepted) = self.child_accepted_tx.lock().unwrap().take() {
                let _ = accepted.send(());
            }
        }
        Ok(())
    }
    fn abandon(self: Arc<Self>) {
        self.abandon_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.panic_if_requested(PANIC_PAYLOAD_ABANDON);
    }
    fn poll_take_outcome(&self, cx: &mut Context<'_>) -> Poll<OperationOutcome<usize, ()>> {
        self.panic_if_requested(PANIC_PAYLOAD_TAKE_OUTCOME);
        assert_eq!(
            self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 16,
            0,
            "poll_take_outcome panic requested"
        );
        self.outcome_polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !self.outcome_ready.load(std::sync::atomic::Ordering::SeqCst) {
            *self.outcome_waker.lock().unwrap() = Some(cx.waker().clone());
            if let Some(sender) = self.driver_waiting_tx.lock().unwrap().take() {
                let _ = sender.send(());
            }
            return Poll::Pending;
        }
        Poll::Ready(
            self.typed_outcome
                .lock()
                .unwrap()
                .take()
                .unwrap_or(OperationOutcome::Succeeded(7)),
        )
    }
}

#[test]
fn test_accepted_external_task_is_bound_activated_and_delivers_its_unique_outcome() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(false),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(false),
        panic_bind_payload: std::sync::atomic::AtomicBool::new(false),
        panic_payload_flags: std::sync::atomic::AtomicUsize::new(0),
        payload_drop_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        abandon_calls: std::sync::atomic::AtomicUsize::new(0),
        activation_thread: std::sync::Mutex::new(None),
        bind_gate: std::sync::Mutex::new(None),
        stop_calls: std::sync::atomic::AtomicUsize::new(0),
        outcome_ready: std::sync::atomic::AtomicBool::new(false),
        outcome_waker: std::sync::Mutex::new(None),
        outcome_polls: std::sync::atomic::AtomicUsize::new(0),
        panic_snapshot: std::sync::atomic::AtomicBool::new(false),
        panic_activate: std::sync::atomic::AtomicBool::new(false),
        panic_flags: std::sync::atomic::AtomicUsize::new(0),
        summary_ready: std::sync::atomic::AtomicBool::new(true),
        summary_waker: std::sync::Mutex::new(None),
        summary_polls: std::sync::atomic::AtomicUsize::new(0),
        summary_kind: std::sync::atomic::AtomicUsize::new(0),
        summary_id: std::sync::Mutex::new(None),
        protocol_fault_id: std::sync::Mutex::new(None),
        typed_outcome: std::sync::Mutex::new(None),
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        domain_settlement_status: std::sync::Mutex::new(None),
        captured_manager: std::sync::Mutex::new(None),
        child_started_tx: std::sync::Mutex::new(None),
        child_accepted_tx: std::sync::Mutex::new(None),
        driver_waiting_tx: std::sync::Mutex::new(None),
        child_handle: std::sync::Mutex::new(None),
    });
    let handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .expect("external task should be admitted");
    wait_for_counter(
        "typed outcome poll",
        || prepared.outcome_polls.load(std::sync::atomic::Ordering::SeqCst),
        || format!("{:?}", handle.observer().snapshot()),
    );
    assert_eq!(
        handle.request_stop(StopReason::UserCancelled),
        StopRequestStatus::Accepted
    );
    prepared.release_outcome();
    let (handle, outcome) = wait_outcome_return_handle_bounded(handle, "stopped external outcome");
    assert!(matches!(outcome, OperationOutcome::Stopped(_)));
    let outcome_polls_after_ready = prepared.outcome_polls.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(prepared.activated.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(prepared.release_polls.load(std::sync::atomic::Ordering::SeqCst) >= 1);
    let polls_without_release = prepared.release_polls.load(std::sync::atomic::Ordering::SeqCst);
    assert!(polls_without_release >= 1, "release port was observed before waiting");
    prepared.release_inputs();
    let report = match handle.wait_settlement_for(std::time::Duration::from_secs(5)).unwrap() {
        SettlementWait::Ready(report) => report,
        SettlementWait::Pending(snapshot) => {
            panic!("settlement watchdog expired; snapshot={snapshot:?}")
        }
    };
    assert_eq!(
        prepared.outcome_polls.load(std::sync::atomic::Ordering::SeqCst),
        outcome_polls_after_ready,
        "the unique typed outcome must not be polled again after Ready"
    );
    let external = report
        .external_settlement
        .as_ref()
        .expect("domain report must be retained");
    assert_eq!(external.operation_id, report.operation_id);
    assert_eq!(external.effects.len(), 1);
    assert_eq!(external.effects_total, 2);
    assert_eq!(external.effect_overflow, 1);
    assert!(report.all_released());
    assert!(prepared.outcome_polls.load(std::sync::atomic::Ordering::SeqCst) >= 1);
    assert!(prepared.stop_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1);
    let second = manager
        .try_submit_external::<usize, (), _>(OperationOptions::default(), Arc::clone(&prepared))
        .err()
        .expect("a prepared Arc is claimed only once");
    let (reason, returned) = second.into_parts();
    assert_eq!(reason, AdmissionError::AlreadyBound);
    assert!(Arc::ptr_eq(&returned, &prepared));
    assert_eq!(prepared.abandon_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn test_post_claim_bind_failure_is_accepted_and_abandoned_once() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(true),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(true),
        panic_bind_payload: std::sync::atomic::AtomicBool::new(false),
        panic_payload_flags: std::sync::atomic::AtomicUsize::new(0),
        payload_drop_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        abandon_calls: std::sync::atomic::AtomicUsize::new(0),
        activation_thread: std::sync::Mutex::new(None),
        bind_gate: std::sync::Mutex::new(None),
        stop_calls: std::sync::atomic::AtomicUsize::new(0),
        outcome_ready: std::sync::atomic::AtomicBool::new(true),
        outcome_waker: std::sync::Mutex::new(None),
        outcome_polls: std::sync::atomic::AtomicUsize::new(0),
        panic_snapshot: std::sync::atomic::AtomicBool::new(false),
        panic_activate: std::sync::atomic::AtomicBool::new(false),
        panic_flags: std::sync::atomic::AtomicUsize::new(0),
        summary_ready: std::sync::atomic::AtomicBool::new(true),
        summary_waker: std::sync::Mutex::new(None),
        summary_polls: std::sync::atomic::AtomicUsize::new(0),
        summary_kind: std::sync::atomic::AtomicUsize::new(0),
        summary_id: std::sync::Mutex::new(None),
        protocol_fault_id: std::sync::Mutex::new(None),
        typed_outcome: std::sync::Mutex::new(None),
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        domain_settlement_status: std::sync::Mutex::new(None),
        captured_manager: std::sync::Mutex::new(None),
        child_started_tx: std::sync::Mutex::new(None),
        child_accepted_tx: std::sync::Mutex::new(None),
        driver_waiting_tx: std::sync::Mutex::new(None),
        child_handle: std::sync::Mutex::new(None),
    });
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .expect("a claimed bind failure remains an accepted operation");
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::ExecutionFailed(_)
    ));
    assert_eq!(prepared.activated.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(prepared.abandon_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    handle.wait_settlement().unwrap();
}

#[test]
fn test_stop_during_bind_wins_before_commit_and_abandons_without_activation() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let proceed = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(true),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(false),
        panic_bind_payload: std::sync::atomic::AtomicBool::new(false),
        panic_payload_flags: std::sync::atomic::AtomicUsize::new(0),
        payload_drop_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        abandon_calls: std::sync::atomic::AtomicUsize::new(0),
        activation_thread: std::sync::Mutex::new(None),
        bind_gate: std::sync::Mutex::new(Some(BindGate {
            entered: entered_tx,
            proceed: Arc::clone(&proceed),
        })),
        stop_calls: std::sync::atomic::AtomicUsize::new(0),
        outcome_ready: std::sync::atomic::AtomicBool::new(true),
        outcome_waker: std::sync::Mutex::new(None),
        outcome_polls: std::sync::atomic::AtomicUsize::new(0),
        panic_snapshot: std::sync::atomic::AtomicBool::new(false),
        panic_activate: std::sync::atomic::AtomicBool::new(false),
        panic_flags: std::sync::atomic::AtomicUsize::new(0),
        summary_ready: std::sync::atomic::AtomicBool::new(true),
        summary_waker: std::sync::Mutex::new(None),
        summary_polls: std::sync::atomic::AtomicUsize::new(0),
        summary_kind: std::sync::atomic::AtomicUsize::new(0),
        summary_id: std::sync::Mutex::new(None),
        protocol_fault_id: std::sync::Mutex::new(None),
        typed_outcome: std::sync::Mutex::new(None),
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        domain_settlement_status: std::sync::Mutex::new(None),
        captured_manager: std::sync::Mutex::new(None),
        child_started_tx: std::sync::Mutex::new(None),
        child_accepted_tx: std::sync::Mutex::new(None),
        driver_waiting_tx: std::sync::Mutex::new(None),
        child_handle: std::sync::Mutex::new(None),
    });
    let submit_manager = manager.clone();
    let submit_prepared = Arc::clone(&prepared);
    let submit = std::thread::spawn(move || {
        submit_manager.try_submit_external::<usize, (), _>(OperationOptions::default(), submit_prepared)
    });
    recv_bounded(&entered_rx, "bind reached after admission", || {
        format!("{:?}", prepared.id.lock().unwrap())
    });
    let _shutdown = manager.request_shutdown(ShutdownMode::Cancel);
    let (ready, changed) = &*proceed;
    *ready.lock().unwrap() = true;
    changed.notify_all();
    let mut handle = join_bounded(submit, "claimed submission after stop", {
        let prepared = Arc::clone(&prepared);
        move || format!("{:?}", prepared.id.lock().unwrap())
    })
    .expect("claimed submission remains accepted after stop");
    assert!(matches!(handle.wait_outcome().unwrap(), OperationOutcome::Stopped(_)));
    assert_eq!(prepared.activated.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(prepared.abandon_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    handle.wait_settlement().unwrap();
}

#[test]
fn test_parent_close_during_bind_keeps_claimed_child_tracked_and_stopped() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (child_manager_tx, child_manager_rx) = std::sync::mpsc::channel();
    let (close_parent_tx, close_parent_rx) = std::sync::mpsc::channel();
    let mut parent = manager
        .submit(OperationOptions::default(), move |scope| {
            child_manager_tx.send(scope.manager()).unwrap();
            recv_bounded(&close_parent_rx, "parent close gate", || {
                "parent body active".to_owned()
            });
            Ok::<_, ()>(())
        })
        .expect("parent operation should be admitted");
    let child_manager = recv_bounded(&child_manager_rx, "child manager handoff", || {
        "parent scope starting".to_owned()
    });

    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let proceed = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(true),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(false),
        panic_bind_payload: std::sync::atomic::AtomicBool::new(false),
        panic_payload_flags: std::sync::atomic::AtomicUsize::new(0),
        payload_drop_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        abandon_calls: std::sync::atomic::AtomicUsize::new(0),
        activation_thread: std::sync::Mutex::new(None),
        bind_gate: std::sync::Mutex::new(Some(BindGate {
            entered: entered_tx,
            proceed: Arc::clone(&proceed),
        })),
        stop_calls: std::sync::atomic::AtomicUsize::new(0),
        outcome_ready: std::sync::atomic::AtomicBool::new(true),
        outcome_waker: std::sync::Mutex::new(None),
        outcome_polls: std::sync::atomic::AtomicUsize::new(0),
        panic_snapshot: std::sync::atomic::AtomicBool::new(false),
        panic_activate: std::sync::atomic::AtomicBool::new(false),
        panic_flags: std::sync::atomic::AtomicUsize::new(0),
        summary_ready: std::sync::atomic::AtomicBool::new(true),
        summary_waker: std::sync::Mutex::new(None),
        summary_polls: std::sync::atomic::AtomicUsize::new(0),
        summary_kind: std::sync::atomic::AtomicUsize::new(0),
        summary_id: std::sync::Mutex::new(None),
        protocol_fault_id: std::sync::Mutex::new(None),
        typed_outcome: std::sync::Mutex::new(None),
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        domain_settlement_status: std::sync::Mutex::new(None),
        captured_manager: std::sync::Mutex::new(None),
        child_started_tx: std::sync::Mutex::new(None),
        child_accepted_tx: std::sync::Mutex::new(None),
        driver_waiting_tx: std::sync::Mutex::new(None),
        child_handle: std::sync::Mutex::new(None),
    });
    let submit = std::thread::spawn({
        let prepared = Arc::clone(&prepared);
        move || child_manager.try_submit_external::<usize, (), _>(OperationOptions::default(), prepared)
    });
    recv_bounded(&entered_rx, "child bind reached final preflight", || {
        format!("parent={:?}", parent.observer().snapshot())
    });
    assert_eq!(
        parent.request_stop(StopReason::UserCancelled),
        StopRequestStatus::Accepted
    );
    close_parent_tx.send(()).unwrap();
    let parent_outcome = parent.wait_outcome().unwrap();
    assert!(matches!(parent_outcome, OperationOutcome::Stopped(_)));
    let (ready, changed) = &*proceed;
    *ready.lock().unwrap() = true;
    changed.notify_all();
    let mut child = join_bounded(submit, "claimed child submission after parent close", {
        let observer = parent.observer();
        move || format!("parent={:?}", observer.snapshot())
    })
    .expect("a claimed child remains accepted while parent closes");
    assert!(matches!(child.wait_outcome().unwrap(), OperationOutcome::Stopped(_)));
    assert_eq!(prepared.activated.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(prepared.abandon_calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    child.wait_settlement().unwrap();
    parent.wait_settlement().unwrap();
}

#[test]
fn test_already_bound_claim_rolls_back_provisional_parent_child_link() {
    let first_manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let second_manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(true),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(false),
        panic_bind_payload: std::sync::atomic::AtomicBool::new(false),
        panic_payload_flags: std::sync::atomic::AtomicUsize::new(0),
        payload_drop_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        abandon_calls: std::sync::atomic::AtomicUsize::new(0),
        activation_thread: std::sync::Mutex::new(None),
        bind_gate: std::sync::Mutex::new(None),
        stop_calls: std::sync::atomic::AtomicUsize::new(0),
        outcome_ready: std::sync::atomic::AtomicBool::new(true),
        outcome_waker: std::sync::Mutex::new(None),
        outcome_polls: std::sync::atomic::AtomicUsize::new(0),
        panic_snapshot: std::sync::atomic::AtomicBool::new(false),
        panic_activate: std::sync::atomic::AtomicBool::new(false),
        panic_flags: std::sync::atomic::AtomicUsize::new(0),
        summary_ready: std::sync::atomic::AtomicBool::new(true),
        summary_waker: std::sync::Mutex::new(None),
        summary_polls: std::sync::atomic::AtomicUsize::new(0),
        summary_kind: std::sync::atomic::AtomicUsize::new(0),
        summary_id: std::sync::Mutex::new(None),
        protocol_fault_id: std::sync::Mutex::new(None),
        typed_outcome: std::sync::Mutex::new(None),
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        domain_settlement_status: std::sync::Mutex::new(None),
        captured_manager: std::sync::Mutex::new(None),
        child_started_tx: std::sync::Mutex::new(None),
        child_accepted_tx: std::sync::Mutex::new(None),
        driver_waiting_tx: std::sync::Mutex::new(None),
        child_handle: std::sync::Mutex::new(None),
    });
    let mut first = first_manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .unwrap();
    assert!(matches!(first.wait_outcome().unwrap(), OperationOutcome::Succeeded(7)));
    first.wait_settlement().unwrap();

    let (child_manager_tx, child_manager_rx) = std::sync::mpsc::channel();
    let (close_parent_tx, close_parent_rx) = std::sync::mpsc::channel();
    let parent = second_manager
        .submit(OperationOptions::default(), move |scope| {
            child_manager_tx.send(scope.manager()).unwrap();
            recv_bounded(&close_parent_rx, "parent close gate", || {
                "parent body active".to_owned()
            });
            Ok::<_, ()>(())
        })
        .unwrap();
    let child_manager = recv_bounded(&child_manager_rx, "child manager handoff", || {
        "parent scope starting".to_owned()
    });
    let rejected = child_manager
        .try_submit_external::<usize, (), _>(OperationOptions::default(), Arc::clone(&prepared))
        .err()
        .expect("cross-manager duplicate claim must be rejected");
    let (reason, returned) = rejected.into_parts();
    assert_eq!(reason, AdmissionError::AlreadyBound);
    assert!(Arc::ptr_eq(&returned, &prepared));
    close_parent_tx.send(()).unwrap();
    parent.wait_settlement().unwrap();
}

#[test]
fn test_dropping_handle_does_not_drop_driver_owner_or_release_obligation() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(false),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(false),
        panic_bind_payload: std::sync::atomic::AtomicBool::new(false),
        panic_payload_flags: std::sync::atomic::AtomicUsize::new(0),
        payload_drop_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        abandon_calls: std::sync::atomic::AtomicUsize::new(0),
        activation_thread: std::sync::Mutex::new(None),
        bind_gate: std::sync::Mutex::new(None),
        stop_calls: std::sync::atomic::AtomicUsize::new(0),
        outcome_ready: std::sync::atomic::AtomicBool::new(true),
        outcome_waker: std::sync::Mutex::new(None),
        outcome_polls: std::sync::atomic::AtomicUsize::new(0),
        panic_snapshot: std::sync::atomic::AtomicBool::new(false),
        panic_activate: std::sync::atomic::AtomicBool::new(false),
        panic_flags: std::sync::atomic::AtomicUsize::new(0),
        summary_ready: std::sync::atomic::AtomicBool::new(true),
        summary_waker: std::sync::Mutex::new(None),
        summary_polls: std::sync::atomic::AtomicUsize::new(0),
        summary_kind: std::sync::atomic::AtomicUsize::new(0),
        summary_id: std::sync::Mutex::new(None),
        protocol_fault_id: std::sync::Mutex::new(None),
        typed_outcome: std::sync::Mutex::new(None),
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        domain_settlement_status: std::sync::Mutex::new(None),
        captured_manager: std::sync::Mutex::new(None),
        child_started_tx: std::sync::Mutex::new(None),
        child_accepted_tx: std::sync::Mutex::new(None),
        driver_waiting_tx: std::sync::Mutex::new(None),
        child_handle: std::sync::Mutex::new(None),
    });
    let handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .unwrap();
    let (handle, outcome) = wait_outcome_return_handle_bounded(handle, "typed outcome before settlement");
    assert!(matches!(outcome, OperationOutcome::Succeeded(7)));
    drop(handle);
    let shutdown = manager.request_shutdown(ShutdownMode::Cancel);
    prepared.release_inputs();
    assert_eq!(shutdown.wait().unwrap().snapshot.active_operations, 0);
    assert_eq!(prepared.activated.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn test_callback_panics_are_captured_and_activation_panic_is_not_abandoned() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let snapshot_panics = Prepared::ready_fixture();
    snapshot_panics
        .panic_snapshot
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut snapshot_handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&snapshot_panics))
        .unwrap();
    assert!(matches!(
        snapshot_handle.wait_outcome().unwrap(),
        OperationOutcome::ExecutionFailed(_)
    ));
    assert_eq!(snapshot_panics.activated.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(
        snapshot_panics.abandon_calls.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    snapshot_handle.wait_settlement().unwrap();

    let activation_panics = Prepared::ready_fixture();
    activation_panics
        .panic_activate
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let mut activation_handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&activation_panics))
        .unwrap();
    assert!(matches!(
        activation_handle.wait_outcome().unwrap(),
        OperationOutcome::ExecutionFailed(_)
    ));
    assert_eq!(activation_panics.activated.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        activation_panics
            .abandon_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    activation_handle.wait_settlement().unwrap();

    let activation_error = Prepared::ready_fixture();
    activation_error
        .panic_flags
        .store(32, std::sync::atomic::Ordering::SeqCst);
    let mut error_handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&activation_error))
        .unwrap();
    assert!(matches!(
        error_handle.wait_outcome().unwrap(),
        OperationOutcome::ExecutionFailed(_)
    ));
    assert_eq!(activation_error.activated.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        activation_error.abandon_calls.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    error_handle.wait_settlement().unwrap();
}

#[test]
fn test_external_payload_request_stop_and_outcome_poll_panics_are_captured() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let stop_panics = Prepared::ready_fixture();
    stop_panics.panic_flags.store(1, std::sync::atomic::Ordering::SeqCst);
    stop_panics
        .panic_payload_flags
        .store(PANIC_PAYLOAD_STOP, std::sync::atomic::Ordering::SeqCst);
    let stop_handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&stop_panics))
        .unwrap();
    assert_eq!(
        stop_handle.request_stop(StopReason::UserCancelled),
        StopRequestStatus::Accepted
    );
    let (outcome, settlement) = wait_handle_bounded(stop_handle);
    assert!(matches!(outcome, OperationOutcome::Stopped(_)));
    assert_eq!(settlement.status, SettlementStatus::Released);
    assert_eq!(
        stop_panics
            .payload_drop_attempts
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );

    for (panic_bit, payload_bit) in [(2, PANIC_PAYLOAD_SUMMARY), (16, PANIC_PAYLOAD_TAKE_OUTCOME)] {
        let prepared = Prepared::ready_fixture();
        prepared
            .panic_flags
            .store(panic_bit, std::sync::atomic::Ordering::SeqCst);
        prepared
            .panic_payload_flags
            .store(payload_bit, std::sync::atomic::Ordering::SeqCst);
        let handle = manager
            .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
            .unwrap();
        let (outcome, settlement) = wait_handle_bounded(handle);
        assert!(matches!(outcome, OperationOutcome::ExecutionFailed(_)));
        assert_eq!(settlement.status, SettlementStatus::Released);
        assert_eq!(
            prepared.payload_drop_attempts.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
    }
}

#[test]
fn test_external_payload_release_and_settlement_panics_keep_scope_pending_without_fabricating_facts() {
    for (panic_bit, payload_bit) in [(4, PANIC_PAYLOAD_RELEASE), (8, PANIC_PAYLOAD_SETTLEMENT)] {
        let manager = OperationManager::new(ManagerOptions {
            sync_executor: ThreadPoolExecutorOptions {
                worker_count: 1,
                ..ThreadPoolExecutorOptions::default()
            },
            ..ManagerOptions::default()
        })
        .unwrap();
        let prepared = Prepared::ready_fixture();
        prepared
            .panic_flags
            .store(panic_bit, std::sync::atomic::Ordering::SeqCst);
        prepared
            .panic_payload_flags
            .store(payload_bit, std::sync::atomic::Ordering::SeqCst);
        let handle = manager
            .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
            .unwrap();
        let observer = handle.observer();
        assert!(matches!(wait_outcome_bounded(handle), OperationOutcome::Succeeded(7)));
        assert_eq!(
            prepared.payload_drop_attempts.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        assert!(matches!(
            observer
                .wait_settlement_for(std::time::Duration::from_millis(10))
                .unwrap(),
            SettlementWait::Pending(_)
        ));
        let shutdown = manager.request_shutdown(ShutdownMode::Drain);
        assert_eq!(shutdown.snapshot().active_operations, 1);
        assert_eq!(prepared.abandon_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
        // Each case intentionally leaves one driver pending because the
        // adapter panicked before supplying required lifecycle evidence.
        // Leak only this bounded test manager so executor teardown cannot wait
        // on the deliberately unresolved task.
        std::mem::forget(shutdown);
        std::mem::forget(observer);
        std::mem::forget(manager);
    }
}

#[test]
fn test_typed_result_waits_for_matching_summary_when_facts_arrive_out_of_order() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Prepared::ready_fixture();
    prepared.summary_ready.store(false, std::sync::atomic::Ordering::SeqCst);
    let handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .unwrap();
    let observer = handle.observer();
    wait_for_counter(
        "summary poll",
        || prepared.summary_polls.load(std::sync::atomic::Ordering::SeqCst),
        || format!("{:?}", observer.snapshot()),
    );
    let mut async_handle = handle.into_async();
    let mut outcome = Box::pin(async_handle.wait_outcome());
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    assert!(
        outcome.as_mut().poll(&mut context).is_pending(),
        "typed outcome stays Pending while matching summary is gated"
    );
    prepared.release_summary();
    assert!(matches!(
        wait_future_bounded(
            outcome.as_mut(),
            &mut context,
            "typed outcome after summary release",
            || format!("{:?}", observer.snapshot()),
        )
        .unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    assert!(matches!(
        observer.wait_settlement_for(std::time::Duration::from_secs(5)).unwrap(),
        SettlementWait::Ready(_)
    ));
}

#[test]
fn test_external_summary_diagnostics_are_free_for_failed_and_stopped_outcomes() {
    for (kind, outcome) in [
        (1, OperationOutcome::Failed(())),
        (
            2,
            OperationOutcome::Stopped(qubit_operation::StopReport {
                reason: StopReason::UserCancelled,
                accepted_at: None,
            }),
        ),
    ] {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let prepared = Prepared::ready_fixture();
        prepared.summary_kind.store(kind, std::sync::atomic::Ordering::SeqCst);
        *prepared.typed_outcome.lock().unwrap() = Some(outcome);
        let mut handle = manager
            .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
            .unwrap();
        match kind {
            1 => assert!(matches!(handle.wait_outcome().unwrap(), OperationOutcome::Failed(()))),
            _ => assert!(matches!(handle.wait_outcome().unwrap(), OperationOutcome::Stopped(_))),
        }
        handle.wait_settlement().unwrap();
    }
}

#[test]
fn test_domain_settlement_is_retained_and_drives_overall_status() {
    for (domain_status, expected_status) in [
        (SettlementStatus::Released, SettlementStatus::Released),
        (SettlementStatus::Transferred, SettlementStatus::Transferred),
        (SettlementStatus::RecoveryRequired, SettlementStatus::RecoveryRequired),
    ] {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let prepared = Prepared::ready_fixture();
        *prepared.domain_settlement_status.lock().unwrap() = Some(domain_status);
        let mut handle = manager
            .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
            .expect("external task should be admitted");

        assert!(matches!(handle.wait_outcome().unwrap(), OperationOutcome::Succeeded(7)));
        let report = handle.wait_settlement().unwrap();
        let external = report
            .external_settlement
            .as_ref()
            .expect("domain evidence must be retained");
        assert_eq!(external.status, domain_status);
        assert_eq!(report.status, expected_status);
        assert_eq!(external.effects.len(), 1);
        assert_eq!(external.effects_total, 2);
        assert_eq!(external.effect_overflow, 1);
        if domain_status == SettlementStatus::Transferred {
            assert_eq!(external.transferred_count, 1);
            assert!(external.transfers_truncated);
        }
        if domain_status == SettlementStatus::RecoveryRequired {
            assert_eq!(external.recovery_count, 1);
            assert!(external.recoveries_truncated);
        }
        assert_eq!(report.transferred_count, 0, "outer counts describe only scope evidence");
        assert_eq!(report.recovery_count, 0, "outer counts describe only scope evidence");
        assert_eq!(report.all_released(), domain_status == SettlementStatus::Released);
    }
}

#[test]
fn test_external_summary_rejects_wrong_classification_and_identity() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let first = Prepared::ready_fixture();
    let first_handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&first))
        .unwrap();
    let foreign_id = first.id.lock().unwrap().expect("first operation is bound");
    let mut first_handle = first_handle;
    assert!(matches!(
        first_handle.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    first_handle.wait_settlement().unwrap();

    let wrong_kind = Prepared::ready_fixture();
    wrong_kind.summary_kind.store(3, std::sync::atomic::Ordering::SeqCst);
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&wrong_kind))
        .unwrap();
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::ExecutionFailed(_)
    ));
    assert!(matches!(
        handle.wait_settlement_for(std::time::Duration::from_secs(5)).unwrap(),
        SettlementWait::Ready(_)
    ));

    let wrong_id = Prepared::ready_fixture();
    *wrong_id.summary_id.lock().unwrap() = Some(foreign_id);
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&wrong_id))
        .unwrap();
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::ExecutionFailed(_)
    ));
    handle.wait_settlement().unwrap();
}

#[test]
fn test_external_frame_ends_at_typed_outcome_while_release_is_still_pending() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Prepared::ready_fixture();
    prepared.release_ready.store(false, std::sync::atomic::Ordering::SeqCst);
    let handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .unwrap();
    let (handle, outcome) = wait_outcome_return_handle_bounded(handle, "typed outcome before release");
    assert!(matches!(outcome, OperationOutcome::Succeeded(7)));
    let scoped_manager = prepared
        .captured_manager
        .lock()
        .unwrap()
        .take()
        .expect("activate receives the execution frame manager");
    let child = Prepared::ready_fixture();
    let error = scoped_manager
        .try_submit_external::<usize, (), _>(OperationOptions::default(), child)
        .err()
        .expect("typed outcome ends the execution frame before release");
    assert_eq!(error.reason(), AdmissionError::ScopeEnded);
    assert!(matches!(
        handle.wait_settlement_for(std::time::Duration::from_millis(1)).unwrap(),
        SettlementWait::Pending(_)
    ));
    prepared.release_inputs();
    assert!(matches!(
        handle.wait_settlement_for(std::time::Duration::from_secs(5)).unwrap(),
        SettlementWait::Ready(_)
    ));
    let release_polls_after_ready = prepared.release_polls.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        prepared.release_polls.load(std::sync::atomic::Ordering::SeqCst),
        release_polls_after_ready,
        "the driver must not poll release again after Ready"
    );
}

#[test]
fn test_typed_outcome_precedes_settlement_and_ready_settlement_is_not_repolled() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Prepared::ready_fixture();
    prepared
        .settlement_ready
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .unwrap();
    assert!(matches!(handle.wait_outcome().unwrap(), OperationOutcome::Succeeded(7)));
    wait_for_counter(
        "settlement poll",
        || prepared.settlement_polls.load(std::sync::atomic::Ordering::SeqCst),
        || format!("{:?}", handle.observer().snapshot()),
    );
    assert!(matches!(
        handle.wait_settlement_for(std::time::Duration::from_millis(5)).unwrap(),
        SettlementWait::Pending(_)
    ));
    prepared.release_settlement();
    handle.wait_settlement().unwrap();
    let ready_polls = prepared.settlement_polls.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        prepared.settlement_polls.load(std::sync::atomic::Ordering::SeqCst),
        ready_polls,
        "the driver must not poll settlement again after Ready"
    );
}

#[test]
fn test_concurrent_managers_claim_one_shared_arc_once() {
    let first_manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let second_manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let proceed = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let prepared = Prepared::ready_fixture();
    *prepared.bind_gate.lock().unwrap() = Some(BindGate {
        entered: entered_tx,
        proceed: Arc::clone(&proceed),
    });
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let first = std::thread::spawn({
        let manager = first_manager.clone();
        let prepared = Arc::clone(&prepared);
        let barrier = Arc::clone(&barrier);
        move || {
            barrier.wait();
            manager.try_submit_external::<usize, (), _>(OperationOptions::default(), prepared)
        }
    });
    let second = std::thread::spawn({
        let manager = second_manager.clone();
        let prepared = Arc::clone(&prepared);
        let barrier = Arc::clone(&barrier);
        move || {
            barrier.wait();
            manager.try_submit_external::<usize, (), _>(OperationOptions::default(), prepared)
        }
    });
    barrier.wait();
    recv_bounded(&entered_rx, "one bind call acquired the Arc", || {
        format!("prepared_id={:?}", prepared.id.lock().unwrap())
    });
    let (ready, changed) = &*proceed;
    *ready.lock().unwrap() = true;
    changed.notify_all();
    let first = join_bounded(first, "first cross-manager submit", {
        let prepared = Arc::clone(&prepared);
        move || format!("prepared_id={:?}", prepared.id.lock().unwrap())
    });
    let second = join_bounded(second, "second cross-manager submit", {
        let prepared = Arc::clone(&prepared);
        move || format!("prepared_id={:?}", prepared.id.lock().unwrap())
    });
    let mut winner = match (first, second) {
        (Ok(handle), Err(error)) | (Err(error), Ok(handle)) => {
            assert_eq!(error.reason(), AdmissionError::AlreadyBound);
            assert!(Arc::ptr_eq(&error.into_parts().1, &prepared));
            handle
        }
        _ => panic!("exactly one cross-manager submission must claim the Arc"),
    };
    assert!(matches!(winner.wait_outcome().unwrap(), OperationOutcome::Succeeded(7)));
    winner.wait_settlement().unwrap();
    assert_eq!(prepared.activated.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(prepared.abandon_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn test_full_manager_rejects_before_calling_bind() {
    let manager = OperationManager::new(ManagerOptions {
        limits: ManagerLimits {
            active_operations: 1,
            ..ManagerLimits::default()
        },
        ..ManagerOptions::default()
    })
    .unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut held = manager
        .submit(OperationOptions::default(), move |_| {
            started_tx.send(()).unwrap();
            recv_bounded(&release_rx, "release held operation", || {
                "held operation is running".to_owned()
            });
            Ok::<_, ()>(())
        })
        .unwrap();
    recv_bounded(&started_rx, "held operation started", || {
        "manager capacity occupied".to_owned()
    });
    let prepared = Prepared::ready_fixture();
    let error = manager
        .try_submit_external::<usize, (), _>(OperationOptions::default(), Arc::clone(&prepared))
        .err()
        .expect("capacity exhaustion rejects admission");
    assert_eq!(error.reason(), AdmissionError::Busy);
    assert!(
        prepared.id.lock().unwrap().is_none(),
        "bind must not run before capacity admission"
    );
    release_tx.send(()).unwrap();
    assert!(matches!(held.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
    held.wait_settlement().unwrap();
}

struct ThreadWake(std::thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on_bounded<F: Future>(future: F, stage: &str, snapshot: impl Fn() -> String) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                let now = std::time::Instant::now();
                if now >= deadline {
                    panic!("watchdog expired waiting for {stage}; snapshot={}", snapshot());
                }
                std::thread::park_timeout(deadline.saturating_duration_since(now));
            }
        }
    }
}

#[test]
fn test_sync_external_child_can_progress_without_releasing_driver() {
    let options = ManagerOptions {
        sync_executor: ThreadPoolExecutorOptions {
            worker_count: 1,
            queue_capacity: 1,
        },
        ..ManagerOptions::default()
    };
    let manager = OperationManager::new(options).unwrap();
    let probe = Prepared::ready_fixture();
    probe.release_ready.store(false, std::sync::atomic::Ordering::SeqCst);
    probe.outcome_ready.store(false, std::sync::atomic::Ordering::SeqCst);
    probe.summary_ready.store(false, std::sync::atomic::Ordering::SeqCst);
    probe.settlement_ready.store(false, std::sync::atomic::Ordering::SeqCst);
    let (accepted_tx, accepted_rx) = std::sync::mpsc::channel();
    let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    *probe.child_started_tx.lock().unwrap() = Some(started_tx);
    *probe.child_accepted_tx.lock().unwrap() = Some(accepted_tx);
    *probe.driver_waiting_tx.lock().unwrap() = Some(waiting_tx);
    let mut parent = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&probe))
        .unwrap();

    let accepted = recv_optional(
        &accepted_rx,
        std::time::Duration::from_secs(5),
        "child acceptance",
        || format!("{:?}", parent.observer().snapshot()),
    )
    .is_some();
    let driver_waiting = recv_optional(
        &waiting_rx,
        std::time::Duration::from_secs(5),
        "external driver Pending",
        || format!("{:?}", parent.observer().snapshot()),
    )
    .is_some();
    let started_without_escape = recv_optional(
        &started_rx,
        std::time::Duration::from_secs(5),
        "child start before gate release",
        || format!("{:?}", parent.observer().snapshot()),
    )
    .is_some();

    // Keep the independent escape for failure cleanup; a correct async driver
    // lets the accepted sync child provide the facts without it.
    if !started_without_escape {
        probe.release_outcome();
        probe.release_inputs();
        probe.release_summary();
        probe.release_settlement();
    }
    let outcome = parent.wait_outcome();
    let settlement = parent.wait_settlement();
    let child_result = probe
        .child_handle
        .lock()
        .unwrap()
        .take()
        .map(|mut child| child.wait_outcome());
    let started_after_cleanup = started_without_escape
        || recv_optional(
            &started_rx,
            std::time::Duration::from_secs(5),
            "child start after cleanup",
            || format!("{:?}", parent.observer().snapshot()),
        )
        .is_some();

    assert!(accepted, "child was accepted by the parent scope");
    assert!(
        driver_waiting || started_without_escape,
        "driver either registered Pending or the accepted child completed first"
    );
    assert!(
        started_without_escape,
        "child progresses while the external driver is waiting"
    );
    assert!(started_after_cleanup, "child was observed before cleanup completed");
    assert!(matches!(outcome.unwrap(), OperationOutcome::Succeeded(7)));
    settlement.unwrap();
    assert!(matches!(
        child_result.unwrap().unwrap(),
        OperationOutcome::Succeeded(41)
    ));
}

#[test]
fn test_sync_external_rejects_when_async_capacity_is_full_without_claiming_arc() {
    let options = ManagerOptions {
        async_executor: qubit_operation::PortableExecutorOptions {
            worker_count: 1,
            task_capacity: 1,
            polls_per_turn: 1,
        },
        ..ManagerOptions::default()
    };
    let manager = OperationManager::new(options).unwrap();
    let async_manager = manager.as_async();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut blocker = async_manager
        .try_submit(OperationOptions::default(), move |_| async move {
            let _ = started_tx.send(());
            recv_bounded(&release_rx, "release gated async operation", || {
                "async body is blocked at its test gate".to_owned()
            });
            Ok::<(), ()>(())
        })
        .unwrap();
    recv_bounded(&started_rx, "async admission blocker started", || {
        "async executor capacity test".to_owned()
    });

    let prepared = Prepared::ready_fixture();
    let result = manager.try_submit_external(OperationOptions::default(), Arc::clone(&prepared));
    let rejection = match result {
        Err(error) => Some(error.into_parts()),
        Ok(mut accepted) => {
            let _ = accepted.wait_outcome();
            let _ = accepted.wait_settlement();
            None
        }
    };

    release_tx.send(()).unwrap();
    assert!(matches!(
        block_on_bounded(blocker.wait_outcome(), "capacity blocker outcome", || {
            "blocker accepted".to_owned()
        })
        .unwrap(),
        OperationOutcome::Succeeded(())
    ));
    block_on_bounded(blocker.wait_settlement(), "capacity blocker settlement", || {
        "blocker accepted".to_owned()
    });

    assert!(rejection.is_some(), "full async executor rejects external driver");
    let (reason, returned) = rejection.unwrap();
    assert_eq!(reason, AdmissionError::Busy);
    assert!(Arc::ptr_eq(&returned, &prepared));
    assert!(prepared.id.lock().unwrap().is_none(), "rejection occurs before bind");
    assert_eq!(prepared.activated.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn test_sync_external_uses_async_capacity_when_sync_pool_is_busy() {
    let options = ManagerOptions {
        sync_executor: ThreadPoolExecutorOptions {
            worker_count: 1,
            queue_capacity: 1,
        },
        ..ManagerOptions::default()
    };
    let manager = OperationManager::new(options).unwrap();
    let (running_tx, running_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut running = manager
        .try_submit(OperationOptions::default(), move |_| {
            let _ = running_tx.send(());
            recv_bounded(&release_rx, "release gated async operation", || {
                "async body is blocked at its test gate".to_owned()
            });
            Ok::<(), ()>(())
        })
        .unwrap();
    recv_bounded(&running_rx, "sync worker started", || {
        "sync executor capacity test".to_owned()
    });
    let mut queued = manager
        .try_submit(OperationOptions::default(), |_| Ok::<(), ()>(()))
        .unwrap();

    let prepared = Prepared::ready_fixture();
    let submitted = manager.try_submit_external(OperationOptions::default(), Arc::clone(&prepared));
    assert!(
        submitted.is_ok(),
        "external driver is admitted independently of busy sync workers"
    );
    let mut external = submitted.unwrap();
    assert!(matches!(
        external.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    external.wait_settlement().unwrap();
    assert_eq!(prepared.activated.load(std::sync::atomic::Ordering::SeqCst), 1);

    release_tx.send(()).unwrap();
    assert!(matches!(
        running.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert!(matches!(
        queued.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    running.wait_settlement().unwrap();
    queued.wait_settlement().unwrap();
}

#[test]
fn test_async_manager_drives_external_activation_on_portable_executor() {
    let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(true),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(false),
        panic_bind_payload: std::sync::atomic::AtomicBool::new(false),
        panic_payload_flags: std::sync::atomic::AtomicUsize::new(0),
        payload_drop_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        abandon_calls: std::sync::atomic::AtomicUsize::new(0),
        activation_thread: std::sync::Mutex::new(None),
        bind_gate: std::sync::Mutex::new(None),
        stop_calls: std::sync::atomic::AtomicUsize::new(0),
        outcome_ready: std::sync::atomic::AtomicBool::new(true),
        outcome_waker: std::sync::Mutex::new(None),
        outcome_polls: std::sync::atomic::AtomicUsize::new(0),
        panic_snapshot: std::sync::atomic::AtomicBool::new(false),
        panic_activate: std::sync::atomic::AtomicBool::new(false),
        panic_flags: std::sync::atomic::AtomicUsize::new(0),
        summary_ready: std::sync::atomic::AtomicBool::new(true),
        summary_waker: std::sync::Mutex::new(None),
        summary_polls: std::sync::atomic::AtomicUsize::new(0),
        summary_kind: std::sync::atomic::AtomicUsize::new(0),
        summary_id: std::sync::Mutex::new(None),
        protocol_fault_id: std::sync::Mutex::new(None),
        typed_outcome: std::sync::Mutex::new(None),
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        domain_settlement_status: std::sync::Mutex::new(None),
        captured_manager: std::sync::Mutex::new(None),
        child_started_tx: std::sync::Mutex::new(None),
        child_accepted_tx: std::sync::Mutex::new(None),
        driver_waiting_tx: std::sync::Mutex::new(None),
        child_handle: std::sync::Mutex::new(None),
    });
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .expect("async external submission should be admitted");
    assert!(matches!(
        block_on_bounded(handle.wait_outcome(), "async manager external outcome", || format!(
            "outcome_polls={}",
            prepared.outcome_polls.load(std::sync::atomic::Ordering::SeqCst)
        ))
        .unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    assert!(
        prepared
            .activation_thread
            .lock()
            .unwrap()
            .as_deref()
            .is_some_and(|name| name.starts_with("qubit-operation-async-"))
    );
    block_on_bounded(handle.wait_settlement(), "async manager external settlement", || {
        format!(
            "settlement_polls={}",
            prepared.settlement_polls.load(std::sync::atomic::Ordering::SeqCst)
        )
    });
}

#[test]
fn test_parent_authorized_external_child_rejects_root_view_and_returns_prepared() {
    let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Prepared::ready_fixture();

    let error = match manager.try_submit_external_as_child(OperationOptions::default(), Arc::clone(&prepared)) {
        Ok(_) => panic!("root manager has no parent group permits to borrow"),
        Err(error) => error,
    };
    let (reason, returned) = error.into_parts();

    assert_eq!(reason, AdmissionError::ScopeEnded);
    assert!(Arc::ptr_eq(&returned, &prepared));
}

#[test]
fn test_parent_external_child_reuses_draining_group_slot() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(qubit_operation::GroupOptions { capacity: 1 }).unwrap();
    let busy_group = manager.group(qubit_operation::GroupOptions { capacity: 1 }).unwrap();
    let foreign_closed_group = manager.group(qubit_operation::GroupOptions { capacity: 1 }).unwrap();
    let (parent_manager_tx, parent_manager_rx) = std::sync::mpsc::channel();
    let (finish_parent_tx, finish_parent_rx) = std::sync::mpsc::channel();
    let mut parent = group
        .submit(OperationOptions::default(), move |scope| {
            parent_manager_tx.send(scope.manager()).unwrap();
            finish_parent_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let parent_manager = recv_bounded(&parent_manager_rx, "parent manager", || "waiting".to_owned());
    let parent_with_busy_group = parent_manager.with_group(&busy_group).unwrap();
    let parent_with_foreign_group = parent_manager.with_group(&foreign_closed_group).unwrap();
    let (busy_started_tx, busy_started_rx) = std::sync::mpsc::channel();
    let (finish_busy_tx, finish_busy_rx) = std::sync::mpsc::channel();
    let mut busy_operation = busy_group
        .submit(OperationOptions::default(), move |_| {
            busy_started_tx.send(()).unwrap();
            finish_busy_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    recv_bounded(&busy_started_rx, "new-group capacity holder", || "waiting".to_owned());

    let draining = group.request_shutdown(ShutdownMode::Drain);
    assert!(!group.snapshot().accepting);
    let parent_manager = parent_manager
        .with_group(&group)
        .expect("rebinding a parent-owned group is idempotent during drain");
    foreign_closed_group.close_admission();
    assert!(matches!(
        parent_manager.with_group(&foreign_closed_group),
        Err(qubit_operation::GroupBindingError::Closed)
    ));

    let rejected_prepared = Prepared::ready_fixture();
    let error = match parent_with_foreign_group
        .as_async()
        .try_submit_external_as_child(OperationOptions::default(), Arc::clone(&rejected_prepared))
    {
        Ok(_) => panic!("a closed newly attached group must still reject admission"),
        Err(error) => error,
    };
    let (reason, returned) = error.into_parts();
    assert_eq!(reason, AdmissionError::Closed);
    assert!(Arc::ptr_eq(&returned, &rejected_prepared));

    let busy_prepared = Prepared::ready_fixture();
    let error = match parent_with_busy_group
        .as_async()
        .try_submit_external_as_child(OperationOptions::default(), Arc::clone(&busy_prepared))
    {
        Ok(_) => panic!("a full newly attached group must reject admission"),
        Err(error) => error,
    };
    let (reason, returned) = error.into_parts();
    assert_eq!(reason, AdmissionError::Busy);
    assert!(Arc::ptr_eq(&returned, &busy_prepared));
    assert_eq!(group.snapshot().active_operations, 1);
    finish_busy_tx.send(()).unwrap();
    let _ = busy_operation.wait_outcome().unwrap();
    busy_operation.wait_settlement().unwrap();

    let prepared = Prepared::ready_fixture();
    prepared
        .settlement_ready
        .store(false, std::sync::atomic::Ordering::SeqCst);
    prepared.panic_flags.store(256, std::sync::atomic::Ordering::SeqCst);
    let mut child = parent_manager
        .as_async()
        .try_submit_external_as_child(OperationOptions::default(), Arc::clone(&prepared))
        .expect("accepted parent can submit external child during group drain");
    assert!(matches!(
        block_on_bounded(child.wait_outcome(), "parent external child outcome", || {
            "child running".to_owned()
        })
        .unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    let mut concurrent_child = parent_manager
        .as_async()
        .try_submit_external_as_child(OperationOptions::default(), Prepared::ready_fixture())
        .expect("accepted parent can submit concurrent external children");
    assert!(matches!(
        block_on_bounded(concurrent_child.wait_outcome(), "concurrent child outcome", || {
            "child running".to_owned()
        })
        .unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    block_on_bounded(
        concurrent_child.wait_settlement(),
        "concurrent child settlement",
        || "child settling".to_owned(),
    );
    prepared.release_settlement();
    block_on_bounded(child.wait_settlement(), "parent external child settlement", || {
        "child settling".to_owned()
    });
    let recoveries = group.recovery_handles_snapshot();
    assert_eq!(recoveries.len(), 1);
    let recovery_store = qubit_operation::RecoveryStore::new(qubit_operation::RecoveryStoreOptions::default()).unwrap();
    recoveries[0].transfer_to(&recovery_store).unwrap();
    assert!(group.recovery_handles_snapshot().is_empty());

    finish_parent_tx.send(()).unwrap();
    let _ = parent.wait_outcome().unwrap();
    parent.wait_settlement().unwrap();
    let _shutdown_report = draining.wait().unwrap();
    assert_eq!(group.snapshot().active_operations, 0);

    let prepared_after_end = Prepared::ready_fixture();
    let error = match parent_manager
        .as_async()
        .try_submit_external_as_child(OperationOptions::default(), Arc::clone(&prepared_after_end))
    {
        Ok(_) => panic!("ended parent must reject external children"),
        Err(error) => error,
    };
    let (reason, returned) = error.into_parts();
    assert_eq!(reason, AdmissionError::ScopeEnded);
    assert!(Arc::ptr_eq(&returned, &prepared_after_end));
}

#[test]
fn test_group_unsettled_count_covers_claimed_external_bind_before_registration() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(qubit_operation::GroupOptions { capacity: 1 }).unwrap();
    let group_manager = manager.with_group(&group).unwrap();
    let prepared = Prepared::ready_fixture();
    prepared
        .settlement_ready
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let (bind_entered_tx, bind_entered_rx) = std::sync::mpsc::channel();
    let proceed = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    *prepared.bind_gate.lock().unwrap() = Some(BindGate {
        entered: bind_entered_tx,
        proceed: Arc::clone(&proceed),
    });

    let submit = std::thread::spawn({
        let prepared = Arc::clone(&prepared);
        move || group_manager.try_submit_external(OperationOptions::default(), prepared)
    });
    recv_bounded(&bind_entered_rx, "external bind preflight", || {
        format!("{:?}", group.snapshot())
    });
    assert_eq!(group.unsettled_operation_count(), 1);

    let draining = group.request_shutdown(ShutdownMode::Drain);
    assert_eq!(group.unsettled_operation_count(), 1);
    let (ready, changed) = &*proceed;
    *ready.lock().unwrap() = true;
    changed.notify_all();
    let group_for_snapshot = group.clone();
    let mut handle = join_bounded(submit, "claimed external submission after group drain", move || {
        format!("{:?}", group_for_snapshot.snapshot())
    })
    .unwrap();
    assert_eq!(group.unsettled_operation_count(), 1);

    prepared.release_settlement();
    handle.wait_settlement().unwrap();
    assert_eq!(group.unsettled_operation_count(), 0);
    assert_eq!(group.snapshot().active_operations, 1);
    assert!(matches!(handle.wait_outcome().unwrap(), OperationOutcome::Succeeded(7)));
    let _shutdown_report = draining.wait().unwrap();
}

#[test]
fn test_group_unsettled_count_rolls_back_rejected_external_preflight() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(qubit_operation::GroupOptions { capacity: 1 }).unwrap();
    let group_manager = manager.with_group(&group).unwrap();
    let prepared = Prepared::ready_fixture();
    let mut first = group_manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .unwrap();
    first.wait_settlement().unwrap();
    assert_eq!(group.unsettled_operation_count(), 0);
    assert!(matches!(first.wait_outcome().unwrap(), OperationOutcome::Succeeded(7)));

    let rejected = group_manager.try_submit_external(OperationOptions::default(), Arc::clone(&prepared));
    let error = match rejected {
        Ok(_) => panic!("already-bound external task must be rejected"),
        Err(error) => error,
    };
    let (reason, returned) = error.into_parts();
    assert_eq!(reason, AdmissionError::AlreadyBound);
    assert!(Arc::ptr_eq(&returned, &prepared));
    assert_eq!(group.unsettled_operation_count(), 0);
}

#[test]
fn test_cancelled_parent_external_child_returns_prepared_operation() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(qubit_operation::GroupOptions { capacity: 1 }).unwrap();
    let (parent_manager_tx, parent_manager_rx) = std::sync::mpsc::channel();
    let (finish_parent_tx, finish_parent_rx) = std::sync::mpsc::channel();
    let mut parent = group
        .submit(OperationOptions::default(), move |scope| {
            parent_manager_tx.send(scope.manager()).unwrap();
            let _ = finish_parent_rx.recv();
            Ok::<_, ()>(())
        })
        .unwrap();
    let parent_manager = recv_bounded(&parent_manager_rx, "parent manager", || "waiting".to_owned());
    let cancelling = group.request_shutdown(ShutdownMode::Cancel);

    let prepared = Prepared::ready_fixture();
    let error = match parent_manager
        .as_async()
        .try_submit_external_as_child(OperationOptions::default(), Arc::clone(&prepared))
    {
        Ok(_) => panic!("cancelled parent scope must reject external children"),
        Err(error) => error,
    };
    let (reason, returned) = error.into_parts();
    assert_eq!(reason, AdmissionError::Stopped);
    assert!(Arc::ptr_eq(&returned, &prepared));

    finish_parent_tx.send(()).unwrap();
    let _ = parent.wait_outcome().unwrap();
    parent.wait_settlement().unwrap();
    let _shutdown_report = cancelling.wait().unwrap();
}

use qubit_operation::*;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::task::{Wake, Waker};

struct Prepared {
    activated: std::sync::atomic::AtomicUsize,
    id: std::sync::Mutex<Option<OperationId>>,
    release_ready: std::sync::atomic::AtomicBool,
    release_waker: std::sync::Mutex<Option<std::task::Waker>>,
    release_polls: std::sync::atomic::AtomicUsize,
    fail_bind: std::sync::atomic::AtomicBool,
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
    settlement_ready: std::sync::atomic::AtomicBool,
    settlement_waker: std::sync::Mutex<Option<Waker>>,
    settlement_polls: std::sync::atomic::AtomicUsize,
    captured_manager: std::sync::Mutex<Option<OperationManager>>,
}

struct BindGate {
    entered: std::sync::mpsc::Sender<()>,
    proceed: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
}

impl OperationControl for Prepared {
    fn request_stop(&self, _: StopReason) {
        assert_eq!(
            self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 1,
            0,
            "request_stop panic requested"
        );
        self.stop_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn snapshot(&self) -> ChildSnapshot {
        assert!(
            !self
                .panic_snapshot
                .load(std::sync::atomic::Ordering::SeqCst),
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
        assert_eq!(
            self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 2,
            0,
            "poll_outcome panic requested"
        );
        self.summary_polls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !self.summary_ready.load(std::sync::atomic::Ordering::SeqCst) {
            *self.summary_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(ChildOutcomeSummary {
            operation_id: self.id.lock().unwrap().expect("bound before polling"),
            kind: ChildOutcomeKind::Succeeded,
            summary: DiagnosticSummary::new("child succeeded"),
        })
    }
    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        assert_eq!(
            self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 4,
            0,
            "poll_release panic requested"
        );
        self.release_polls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !self.release_ready.load(std::sync::atomic::Ordering::SeqCst) {
            *self.release_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(ChildRelease {
            operation_id: self.id.lock().unwrap().expect("bound before polling"),
            confirmed_at: None,
        })
    }
    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>> {
        assert_eq!(
            self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 8,
            0,
            "poll_settlement panic requested"
        );
        self.settlement_polls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !self
            .settlement_ready
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            *self.settlement_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let operation_id = self.id.lock().unwrap().expect("bound before polling");
        Poll::Ready(Arc::new(SettlementReport {
            operation_id,
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

impl Prepared {
    fn release_settlement(&self) {
        self.settlement_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = self.settlement_waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn release_summary(&self) {
        self.summary_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
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
            settlement_ready: std::sync::atomic::AtomicBool::new(true),
            settlement_waker: std::sync::Mutex::new(None),
            settlement_polls: std::sync::atomic::AtomicUsize::new(0),
            captured_manager: std::sync::Mutex::new(None),
        })
    }

    fn release_outcome(&self) {
        self.outcome_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = self.outcome_waker.lock().unwrap().take() {
            waker.wake();
        }
    }

    fn release_inputs(&self) {
        self.release_ready
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(waker) = self.release_waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

impl PreparedExternalOperation<usize, ()> for Prepared {
    fn bind(&self, id: OperationId) -> Result<(), ExternalBindError> {
        let mut bound = self.id.lock().unwrap();
        if bound.is_some() {
            return Err(ExternalBindError::AlreadyBound);
        }
        *bound = Some(id);
        if let Some(gate) = self.bind_gate.lock().unwrap().take() {
            let _ = gate.entered.send(());
            let (ready, changed) = &*gate.proceed;
            let mut ready = ready.lock().unwrap();
            while !*ready {
                ready = changed.wait(ready).unwrap();
            }
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
        self.activated
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        assert!(
            !self
                .panic_activate
                .load(std::sync::atomic::Ordering::SeqCst),
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
        Ok(())
    }
    fn abandon(self: Arc<Self>) {
        self.abandon_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn poll_take_outcome(&self, cx: &mut Context<'_>) -> Poll<OperationOutcome<usize, ()>> {
        assert_eq!(
            self.panic_flags.load(std::sync::atomic::Ordering::SeqCst) & 16,
            0,
            "poll_take_outcome panic requested"
        );
        self.outcome_polls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !self.outcome_ready.load(std::sync::atomic::Ordering::SeqCst) {
            *self.outcome_waker.lock().unwrap() = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(OperationOutcome::Succeeded(7))
    }
}

#[test]
fn accepted_external_task_is_bound_activated_and_delivers_its_unique_outcome() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(false),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(false),
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
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        captured_manager: std::sync::Mutex::new(None),
    });
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .expect("external task should be admitted");
    while prepared
        .outcome_polls
        .load(std::sync::atomic::Ordering::SeqCst)
        == 0
    {
        std::thread::yield_now();
    }
    assert_eq!(
        handle.request_stop(StopReason::UserCancelled),
        StopRequestStatus::Accepted
    );
    prepared.release_outcome();
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::Stopped(_)
    ));
    assert_eq!(
        prepared.activated.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert!(
        prepared
            .release_polls
            .load(std::sync::atomic::Ordering::SeqCst)
            >= 1
    );
    for _ in 0..1000 {
        std::thread::yield_now();
    }
    let polls_without_release = prepared
        .release_polls
        .load(std::sync::atomic::Ordering::SeqCst);
    for _ in 0..1000 {
        std::thread::yield_now();
    }
    assert_eq!(
        prepared
            .release_polls
            .load(std::sync::atomic::Ordering::SeqCst),
        polls_without_release,
        "the retained driver must wait for a wake instead of busy polling"
    );
    prepared.release_inputs();
    handle.wait_settlement().unwrap();
    let outcome_polls_after_ready = prepared
        .outcome_polls
        .load(std::sync::atomic::Ordering::SeqCst);
    for _ in 0..1000 {
        std::thread::yield_now();
    }
    assert_eq!(
        prepared
            .outcome_polls
            .load(std::sync::atomic::Ordering::SeqCst),
        outcome_polls_after_ready,
        "the driver must never poll the unique typed outcome after Ready"
    );
    assert!(
        prepared
            .stop_calls
            .load(std::sync::atomic::Ordering::SeqCst)
            >= 1
    );
    let second = manager
        .try_submit_external::<usize, (), _>(OperationOptions::default(), Arc::clone(&prepared))
        .err()
        .expect("a prepared Arc is claimed only once");
    let (reason, returned) = second.into_parts();
    assert_eq!(reason, AdmissionError::AlreadyBound);
    assert!(Arc::ptr_eq(&returned, &prepared));
    assert_eq!(
        prepared
            .abandon_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[test]
fn post_claim_bind_failure_is_accepted_and_abandoned_once() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(true),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(true),
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
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        captured_manager: std::sync::Mutex::new(None),
    });
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .expect("a claimed bind failure remains an accepted operation");
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::ExecutionFailed(_)
    ));
    assert_eq!(
        prepared.activated.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        prepared
            .abandon_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    handle.wait_settlement().unwrap();
}

#[test]
fn stop_during_bind_wins_before_commit_and_abandons_without_activation() {
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
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        captured_manager: std::sync::Mutex::new(None),
    });
    let submit_manager = manager.clone();
    let submit_prepared = Arc::clone(&prepared);
    let submit = std::thread::spawn(move || {
        submit_manager
            .try_submit_external::<usize, (), _>(OperationOptions::default(), submit_prepared)
    });
    entered_rx.recv().expect("bind reached after admission");
    let _shutdown = manager.request_shutdown(ShutdownMode::Cancel);
    let (ready, changed) = &*proceed;
    *ready.lock().unwrap() = true;
    changed.notify_all();
    let mut handle = submit
        .join()
        .expect("submission thread did not panic")
        .expect("claimed submission remains accepted after stop");
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::Stopped(_)
    ));
    assert_eq!(
        prepared.activated.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        prepared
            .abandon_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    handle.wait_settlement().unwrap();
}

#[test]
fn parent_close_during_bind_keeps_claimed_child_tracked_and_stopped() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (child_manager_tx, child_manager_rx) = std::sync::mpsc::channel();
    let (close_parent_tx, close_parent_rx) = std::sync::mpsc::channel();
    let parent = manager
        .submit(OperationOptions::default(), move |scope| {
            child_manager_tx.send(scope.manager()).unwrap();
            close_parent_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .expect("parent operation should be admitted");
    let child_manager = child_manager_rx.recv().unwrap();

    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let proceed = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(true),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(false),
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
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        captured_manager: std::sync::Mutex::new(None),
    });
    let submit = std::thread::spawn({
        let prepared = Arc::clone(&prepared);
        move || {
            child_manager.try_submit_external::<usize, (), _>(OperationOptions::default(), prepared)
        }
    });
    entered_rx
        .recv()
        .expect("child bind reached final preflight");
    close_parent_tx.send(()).unwrap();
    let (ready, changed) = &*proceed;
    *ready.lock().unwrap() = true;
    changed.notify_all();
    let mut child = submit
        .join()
        .expect("child submission thread did not panic")
        .expect("a claimed child remains accepted while parent closes");
    assert!(matches!(
        child.wait_outcome().unwrap(),
        OperationOutcome::Stopped(_)
    ));
    assert_eq!(
        prepared.activated.load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        prepared
            .abandon_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    child.wait_settlement().unwrap();
    parent.wait_settlement().unwrap();
}

#[test]
fn already_bound_claim_rolls_back_provisional_parent_child_link() {
    let first_manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let second_manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(true),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(false),
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
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        captured_manager: std::sync::Mutex::new(None),
    });
    let mut first = first_manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .unwrap();
    assert!(matches!(
        first.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    first.wait_settlement().unwrap();

    let (child_manager_tx, child_manager_rx) = std::sync::mpsc::channel();
    let (close_parent_tx, close_parent_rx) = std::sync::mpsc::channel();
    let parent = second_manager
        .submit(OperationOptions::default(), move |scope| {
            child_manager_tx.send(scope.manager()).unwrap();
            close_parent_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let child_manager = child_manager_rx.recv().unwrap();
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
fn dropping_handle_does_not_drop_driver_owner_or_release_obligation() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(false),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(false),
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
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        captured_manager: std::sync::Mutex::new(None),
    });
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .unwrap();
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    drop(handle);
    let shutdown = manager.request_shutdown(ShutdownMode::Cancel);
    prepared.release_inputs();
    assert_eq!(shutdown.wait().unwrap().snapshot.active_operations, 0);
    assert_eq!(
        prepared.activated.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
}

#[test]
fn callback_panics_are_captured_and_activation_panic_is_not_abandoned() {
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
    assert_eq!(
        snapshot_panics
            .activated
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    assert_eq!(
        snapshot_panics
            .abandon_calls
            .load(std::sync::atomic::Ordering::SeqCst),
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
    assert_eq!(
        activation_panics
            .activated
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
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
    assert_eq!(
        activation_error
            .activated
            .load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        activation_error
            .abandon_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    error_handle.wait_settlement().unwrap();
}

#[test]
fn request_stop_and_outcome_poll_panics_are_captured() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let stop_panics = Prepared::ready_fixture();
    stop_panics
        .panic_flags
        .store(1, std::sync::atomic::Ordering::SeqCst);
    let mut stop_handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&stop_panics))
        .unwrap();
    assert_eq!(
        stop_handle.request_stop(StopReason::UserCancelled),
        StopRequestStatus::Accepted
    );
    assert!(matches!(
        stop_handle.wait_outcome().unwrap(),
        OperationOutcome::Stopped(_)
    ));
    stop_handle.wait_settlement().unwrap();

    for panic_bit in [2, 16] {
        let prepared = Prepared::ready_fixture();
        prepared
            .panic_flags
            .store(panic_bit, std::sync::atomic::Ordering::SeqCst);
        let mut handle = manager
            .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
            .unwrap();
        assert!(matches!(
            handle.wait_outcome().unwrap(),
            OperationOutcome::ExecutionFailed(_)
        ));
        handle.wait_settlement().unwrap();
    }
}

#[test]
fn release_and_settlement_poll_panics_keep_scope_pending_without_fabricating_facts() {
    for panic_bit in [4, 8] {
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
        let mut handle = manager
            .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
            .unwrap();
        assert!(matches!(
            handle.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(7)
        ));
        assert!(matches!(
            handle
                .wait_settlement_for(std::time::Duration::from_millis(10))
                .unwrap(),
            SettlementWait::Pending(_)
        ));
        let shutdown = manager.request_shutdown(ShutdownMode::Drain);
        assert_eq!(shutdown.snapshot().active_operations, 1);
        assert_eq!(
            prepared
                .abandon_calls
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        drop(handle);

        // Each case intentionally leaves one driver pending because the
        // adapter panicked before supplying required lifecycle evidence.
        // Leak only this bounded test manager so executor teardown cannot wait
        // on the deliberately unresolved task.
        std::mem::forget(shutdown);
        std::mem::forget(manager);
    }
}

#[test]
fn typed_result_waits_for_matching_summary_when_facts_arrive_out_of_order() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Prepared::ready_fixture();
    prepared
        .summary_ready
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .unwrap();
    while prepared
        .summary_polls
        .load(std::sync::atomic::Ordering::SeqCst)
        == 0
    {
        std::thread::yield_now();
    }
    let (outcome_tx, outcome_rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let mut handle = handle;
        let result = handle.wait_outcome();
        let _ = outcome_tx.send(result);
        handle.wait_settlement().unwrap();
    });
    assert!(
        outcome_rx
            .recv_timeout(std::time::Duration::from_millis(10))
            .is_err()
    );
    prepared.release_summary();
    assert!(matches!(
        outcome_rx.recv().unwrap().unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    waiter.join().unwrap();
}

#[test]
fn external_frame_ends_at_typed_outcome_while_release_is_still_pending() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Prepared::ready_fixture();
    prepared
        .release_ready
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .unwrap();
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(7)
    ));
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
        handle
            .wait_settlement_for(std::time::Duration::from_millis(1))
            .unwrap(),
        SettlementWait::Pending(_)
    ));
    prepared.release_inputs();
    handle.wait_settlement().unwrap();
    let release_polls_after_ready = prepared
        .release_polls
        .load(std::sync::atomic::Ordering::SeqCst);
    for _ in 0..1000 {
        std::thread::yield_now();
    }
    assert_eq!(
        prepared
            .release_polls
            .load(std::sync::atomic::Ordering::SeqCst),
        release_polls_after_ready,
        "the driver must not poll release again after Ready"
    );
}

#[test]
fn typed_outcome_precedes_settlement_and_ready_settlement_is_not_repolled() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Prepared::ready_fixture();
    prepared
        .settlement_ready
        .store(false, std::sync::atomic::Ordering::SeqCst);
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .unwrap();
    assert!(matches!(
        handle.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    while prepared
        .settlement_polls
        .load(std::sync::atomic::Ordering::SeqCst)
        == 0
    {
        std::thread::yield_now();
    }
    assert!(matches!(
        handle
            .wait_settlement_for(std::time::Duration::from_millis(5))
            .unwrap(),
        SettlementWait::Pending(_)
    ));
    prepared.release_settlement();
    handle.wait_settlement().unwrap();
    let ready_polls = prepared
        .settlement_polls
        .load(std::sync::atomic::Ordering::SeqCst);
    for _ in 0..1000 {
        std::thread::yield_now();
    }
    assert_eq!(
        prepared
            .settlement_polls
            .load(std::sync::atomic::Ordering::SeqCst),
        ready_polls,
        "the driver must not poll settlement again after Ready"
    );
}

#[test]
fn concurrent_managers_claim_one_shared_arc_once() {
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
    entered_rx.recv().expect("one bind call acquired the Arc");
    let (ready, changed) = &*proceed;
    *ready.lock().unwrap() = true;
    changed.notify_all();
    let first = first.join().unwrap();
    let second = second.join().unwrap();
    let mut winner = match (first, second) {
        (Ok(handle), Err(error)) | (Err(error), Ok(handle)) => {
            assert_eq!(error.reason(), AdmissionError::AlreadyBound);
            assert!(Arc::ptr_eq(&error.into_parts().1, &prepared));
            handle
        }
        _ => panic!("exactly one cross-manager submission must claim the Arc"),
    };
    assert!(matches!(
        winner.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    winner.wait_settlement().unwrap();
    assert_eq!(
        prepared.activated.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    assert_eq!(
        prepared
            .abandon_calls
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
}

#[test]
fn full_manager_rejects_before_calling_bind() {
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
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    started_rx.recv().unwrap();
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
    assert!(matches!(
        held.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
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

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

#[test]
fn async_manager_drives_external_activation_on_portable_executor() {
    let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
    let prepared = Arc::new(Prepared {
        activated: std::sync::atomic::AtomicUsize::new(0),
        id: std::sync::Mutex::new(None),
        release_ready: std::sync::atomic::AtomicBool::new(true),
        release_waker: std::sync::Mutex::new(None),
        release_polls: std::sync::atomic::AtomicUsize::new(0),
        fail_bind: std::sync::atomic::AtomicBool::new(false),
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
        settlement_ready: std::sync::atomic::AtomicBool::new(true),
        settlement_waker: std::sync::Mutex::new(None),
        settlement_polls: std::sync::atomic::AtomicUsize::new(0),
        captured_manager: std::sync::Mutex::new(None),
    });
    let mut handle = manager
        .try_submit_external(OperationOptions::default(), Arc::clone(&prepared))
        .expect("async external submission should be admitted");
    assert!(matches!(
        block_on(handle.wait_outcome()).unwrap(),
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
    block_on(handle.wait_settlement());
}

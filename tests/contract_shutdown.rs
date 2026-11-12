use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::task::{Context, Poll, Wake, Waker};
#[path = "support/gate.rs"]
mod gate;
#[path = "support/clock.rs"]
mod manual_clock;

struct GateCleanup {
    gate: gate::Gate,
    started: std::sync::Arc<AtomicBool>,
}

struct RecoveryCleanup {
    id: Arc<std::sync::Mutex<Option<qubit_operation::report::ResourceId>>>,
    registry: qubit_operation::resource::RecoveryRegistry,
}

impl qubit_operation::protocol::OwnedCleanup for RecoveryCleanup {
    fn begin(
        self: Box<Self>,
        _: qubit_operation::protocol::CleanupContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = qubit_operation::report::CleanupOutcome>
                + Send
                + 'static,
        >,
    > {
        let id = self
            .id
            .lock()
            .unwrap()
            .expect("resource ID set before cleanup");
        let (_, owner) = self
            .registry
            .register_with_capsule(id, String::from("capsule"), String::from("failure owner"))
            .unwrap();
        let failure = qubit_operation::report::CleanupFailure {
            resource_id: id,
            stage: qubit_operation::report::CleanupStage::Removing,
            summary: qubit_operation::report::DiagnosticSummary::new("recovery owner retained"),
            source: None,
        };
        Box::pin(std::future::ready(
            qubit_operation::report::CleanupOutcome::FailedWithRecovery(failure, owner),
        ))
    }
}

struct ReadyControl(qubit_operation::report::OperationId);
impl qubit_operation::protocol::ManagedOperationControl for ReadyControl {
    fn request_stop(&self, _: qubit_operation::report::StopReason) {}
    fn snapshot(&self) -> qubit_operation::report::ChildSnapshot {
        qubit_operation::report::ChildSnapshot {
            operation_id: self.0,
            phase: qubit_operation::report::OperationPhase::ResultReady,
            resource_phase: qubit_operation::report::ResourcePhase::Released,
            stop_requested: false,
            released: true,
        }
    }
    fn poll_business(
        &self,
        _: &mut Context<'_>,
    ) -> Poll<qubit_operation::report::ChildBusinessSummary> {
        Poll::Ready(qubit_operation::report::ChildBusinessSummary {
            operation_id: self.0,
            outcome: qubit_operation::report::ChildBusinessOutcome::Succeeded,
            summary: qubit_operation::report::DiagnosticSummary::new("ready child"),
        })
    }
    fn poll_released(&self, _: &mut Context<'_>) -> Poll<qubit_operation::report::ChildSettlement> {
        Poll::Ready(qubit_operation::report::ChildSettlement {
            operation_id: self.0,
            released: true,
            summary: qubit_operation::report::DiagnosticSummary::new("released child"),
        })
    }
}
impl qubit_operation::protocol::OwnedCleanup for GateCleanup {
    fn begin(
        self: Box<Self>,
        _: qubit_operation::protocol::CleanupContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = qubit_operation::report::CleanupOutcome>
                + Send
                + 'static,
        >,
    > {
        Box::pin(async move {
            self.started.store(true, Ordering::SeqCst);
            self.gate.wait().await;
            qubit_operation::report::CleanupOutcome::Cleaned
        })
    }
}

struct ThreadWake(std::thread::Thread);
impl Wake for ThreadWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.unpark();
    }
}
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let waker = Waker::from(std::sync::Arc::new(ThreadWake(std::thread::current())));
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
fn service_group_drain_does_not_close_shared_supervisor() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let group = supervisor.service_group(4).unwrap();
    let other = supervisor.service_group(4).unwrap();
    let gate = gate::Gate::new();
    let other_gate = gate::Gate::new();
    let (tx, rx) = mpsc::channel();
    let (other_tx, other_rx) = mpsc::channel();
    let body_gate = gate.clone();
    let mut operation = group
        .start(ScopeOptions::default(), move |_| async move {
            tx.send(()).unwrap();
            body_gate.wait().await;
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
    let other_body_gate = other_gate.clone();
    let mut other_operation = other
        .start(ScopeOptions::default(), move |_| async move {
            other_tx.send(()).unwrap();
            other_body_gate.wait().await;
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
    rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
    other_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    gate.wait_started();
    other_gate.wait_started();
    assert!(gate.started());

    let report = group.shutdown_blocking(ShutdownMode::Cancel, std::time::Duration::from_millis(1));
    assert_eq!(report.pending_requests, 1);
    assert_eq!(other.active_requests(), 1);
    assert!(!other_gate.is_released());
    gate.release();
    assert!(
        group
            .shutdown_blocking(ShutdownMode::Cancel, std::time::Duration::from_secs(1))
            .is_complete()
    );
    assert!(supervisor.try_admit().is_ok());
    assert!(matches!(
        operation.result_blocking().unwrap(),
        qubit_operation::report::OperationOutcome::Stopped(_)
    ));
    assert_eq!(other.active_requests(), 1);
    other_gate.release();
    assert!(
        other
            .shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(1))
            .is_complete()
    );
    assert!(matches!(
        other_operation.result_blocking().unwrap(),
        qubit_operation::report::OperationOutcome::Succeeded(())
    ));
}

#[test]
fn global_pending_request_count_sums_independent_service_groups_without_root_double_count() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let first = supervisor.service_group(2).unwrap();
    let second = supervisor.service_group(2).unwrap();
    let request_a = first.try_admit().unwrap();
    let request_b = second.try_admit().unwrap();
    let report =
        supervisor.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_millis(1));
    assert_eq!(report.pending_requests, 2);
    drop(request_a);
    drop(request_b);
}

#[test]
fn sequential_requests_do_not_consume_permanent_root_capacity() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let group = supervisor.service_group(2).unwrap();
    for _ in 0..80 {
        let mut request = group
            .start(ScopeOptions::default(), |_| async {
                Ok::<_, std::convert::Infallible>(())
            })
            .unwrap();
        assert!(matches!(
            request.result_blocking().unwrap(),
            qubit_operation::report::OperationOutcome::Succeeded(())
        ));
        assert!(request.released_blocking().unwrap().all_released);
    }
    assert_eq!(group.active_requests(), 0);
    assert!(
        supervisor
            .shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(1))
            .is_complete()
    );
    assert_eq!(supervisor.active_roots(), 0);
}

#[test]
fn drain_timeout_keeps_work_alive_then_cancel_upgrade_waits_for_release() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let (started_tx, started_rx) = mpsc::channel();
    let gate = gate::Gate::new();
    let body_gate = gate.clone();
    let mut handle = supervisor
        .start::<_, _, (), std::convert::Infallible>(ScopeOptions::default(), move |_| async move {
            started_tx.send(()).unwrap();
            body_gate.wait().await;
            Ok(())
        })
        .unwrap();
    started_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .unwrap();
    gate.wait_started();

    let pending =
        supervisor.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_millis(1));
    assert_eq!(pending.pending_requests, 1);
    let pending =
        supervisor.shutdown_blocking(ShutdownMode::Cancel, std::time::Duration::from_millis(1));
    assert_eq!(pending.pending_requests, 1);
    gate.release();
    let complete =
        supervisor.shutdown_blocking(ShutdownMode::Cancel, std::time::Duration::from_secs(2));
    assert!(complete.is_complete());
    assert_eq!(
        handle
            .observer()
            .released_blocking()
            .unwrap()
            .resource_phase,
        qubit_operation::report::ResourcePhase::Released
    );
    assert!(matches!(
        handle.result_blocking().unwrap(),
        qubit_operation::report::OperationOutcome::Stopped(_)
    ));
    drop(handle);
}

#[test]
fn async_shutdown_returns_reusable_observer_after_timeout() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let gate = gate::Gate::new();
    let body_gate = gate.clone();
    let handle = supervisor
        .start::<_, _, (), std::convert::Infallible>(ScopeOptions::default(), move |_| async move {
            body_gate.wait().await;
            Ok(())
        })
        .unwrap();
    gate.wait_started();
    use qubit_operation::context::Deadline;
    use qubit_operation::report::{ClockDomainId, MonotonicTime};
    let clock = std::sync::Arc::new(manual_clock::ManualClock::new(ClockDomainId(907)));
    let timer_clock = clock.clone();
    let timer = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(10));
        timer_clock.advance_by(1);
    });
    let pending = block_on(supervisor.shutdown_async(
        ShutdownMode::Drain,
        Deadline::at(MonotonicTime::new(ClockDomainId(907), 1)),
        clock.clone(),
    ))
    .unwrap();
    timer.join().unwrap();
    assert_eq!(clock.pending_timers(), 0);
    assert_eq!(pending.report.pending_requests, 1);
    gate.release();
    let complete = block_on(pending.observer.wait_async(
        Deadline::at(MonotonicTime::new(ClockDomainId(907), 2)),
        clock,
    ))
    .unwrap();
    assert!(complete.report.is_complete());
    assert!(matches!(
        handle
            .observer()
            .released_blocking()
            .unwrap()
            .resource_phase,
        qubit_operation::report::ResourcePhase::Released
    ));
}

#[test]
fn dropping_last_public_supervisor_clone_cancels_but_observer_does_not_keep_it_public() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let another_public = supervisor.clone();
    let gate = gate::Gate::new();
    let body_gate = gate.clone();
    let mut operation = supervisor
        .start::<_, _, (), std::convert::Infallible>(ScopeOptions::default(), move |_| async move {
            body_gate.wait().await;
            Ok(())
        })
        .unwrap();
    gate.wait_started();
    let observer = operation.observer();
    drop(supervisor);
    drop(another_public);
    gate.release();
    assert!(matches!(
        operation.result_blocking().unwrap(),
        qubit_operation::report::OperationOutcome::Stopped(_)
    ));
    assert_eq!(
        observer.released_blocking().unwrap().resource_phase,
        qubit_operation::report::ResourcePhase::Released
    );
}

#[test]
fn service_resource_scope_has_independent_quota_and_cleanup_is_kept_alive() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let group = supervisor.service_group(2).unwrap();
    let scope = group.resource_scope(1).unwrap();
    let cleanup_gate = gate::Gate::new();
    let started = std::sync::Arc::new(AtomicBool::new(false));
    let id = scope
        .register_resource(GateCleanup {
            gate: cleanup_gate.clone(),
            started: started.clone(),
        })
        .unwrap();
    let rejected = scope
        .register_resource(GateCleanup {
            gate: gate::Gate::new(),
            started: Arc::new(AtomicBool::new(false)),
        })
        .unwrap_err();
    assert_eq!(
        rejected.reason,
        qubit_operation::report::AdmissionError::ReservationLimit
    );
    drop(rejected.into_owner());
    assert_eq!(supervisor.active_roots(), 0);

    let pending =
        group.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_millis(10));
    assert_eq!(pending.pending_resources, 1);
    assert!(started.load(Ordering::SeqCst));
    assert!(matches!(
        scope.acquire_use(id),
        Err(qubit_operation::resource::ResourceError::ClosedForUse)
    ));
    assert!(!cleanup_gate.is_released());
    cleanup_gate.release();
    cleanup_gate.wait_released();
    assert!(
        group
            .shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(1))
            .is_complete()
    );
}

#[test]
fn global_cancel_closes_service_groups_and_waits_for_service_resource_cleanup() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let group = supervisor.service_group(2).unwrap();
    let scope = group.resource_scope(1).unwrap();
    let cleanup_gate = gate::Gate::new();
    let started = Arc::new(AtomicBool::new(false));
    scope
        .register_resource(GateCleanup {
            gate: cleanup_gate.clone(),
            started: started.clone(),
        })
        .unwrap();

    let pending =
        supervisor.shutdown_blocking(ShutdownMode::Cancel, std::time::Duration::from_millis(10));
    assert_eq!(pending.pending_resources, 1);
    cleanup_gate.wait_started();
    assert!(started.load(Ordering::SeqCst));
    assert!(matches!(
        group.try_admit(),
        Err(qubit_operation::report::AdmissionError::Closed)
    ));
    cleanup_gate.release();
    cleanup_gate.wait_released();
    assert!(
        supervisor
            .shutdown_blocking(ShutdownMode::Cancel, std::time::Duration::from_secs(1))
            .is_complete()
    );
}

#[test]
fn unresolved_recovery_owner_is_reported_as_shutdown_responsibility() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let group = supervisor.service_group(2).unwrap();
    let id_slot = Arc::new(std::sync::Mutex::new(None));
    let cleanup_registry = qubit_operation::resource::RecoveryRegistry::new(1);
    let scope_slot = Arc::new(std::sync::Mutex::new(None));
    let id_for_body = Arc::clone(&id_slot);
    let scope_for_body = Arc::clone(&scope_slot);
    let registry_for_cleanup = cleanup_registry.clone();
    let mut operation = group
        .start(ScopeOptions::default(), move |scope| async move {
            *scope_for_body.lock().unwrap() = Some(scope.clone());
            let id = scope
                .register_resource(RecoveryCleanup {
                    id: Arc::clone(&id_for_body),
                    registry: registry_for_cleanup,
                })
                .unwrap();
            *id_for_body.lock().unwrap() = Some(id);
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
    assert!(matches!(
        operation.result_blocking().unwrap(),
        qubit_operation::report::OperationOutcome::Succeeded(())
    ));
    let settlement = operation.released_blocking().unwrap();
    assert_eq!(
        settlement.resource_phase,
        qubit_operation::report::ResourcePhase::RecoveryRequired
    );
    let report =
        supervisor.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(1));
    assert_eq!(report.pending_recovery, 1);
    assert!(!report.is_complete());
    let scope = scope_slot.lock().unwrap().take().unwrap();
    let key = scope.take_recovery_key().unwrap();
    drop(scope.take_recovery(key).unwrap());
    let complete = supervisor
        .shutdown_observer()
        .wait_blocking(std::time::Duration::from_secs(1));
    assert!(complete.report.is_complete());
}

#[test]
fn drain_allows_existing_root_to_commit_children_but_cancel_rejects_commit() {
    for (mode, expected_commit) in [(ShutdownMode::Drain, true), (ShutdownMode::Cancel, false)] {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let gate = gate::Gate::new();
        let body_gate = gate.clone();
        let (commit_tx, commit_rx) = mpsc::channel();
        let mut operation = supervisor
            .start::<_, _, (), ()>(ScopeOptions::default(), move |scope| async move {
                body_gate.wait().await;
                let mut reservation = scope
                    .reserve_child(qubit_operation::scope::ChildRequirement::Required)
                    .unwrap();
                reservation
                    .attach(Arc::new(ReadyControl(
                        qubit_operation::report::OperationId::new(900_000),
                    )))
                    .unwrap();
                let committed = reservation.commit().is_ok();
                commit_tx.send(committed).unwrap();
                Ok(())
            })
            .unwrap();
        gate.wait_started();
        assert_eq!(
            supervisor
                .shutdown_blocking(mode, std::time::Duration::from_millis(1))
                .pending_requests,
            1
        );
        gate.release();
        assert_eq!(
            commit_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap(),
            expected_commit
        );
        assert!(
            supervisor
                .shutdown_blocking(mode, std::time::Duration::from_secs(1))
                .is_complete()
        );
        if expected_commit {
            assert!(matches!(
                operation.result_blocking().unwrap(),
                qubit_operation::report::OperationOutcome::Succeeded(())
            ));
        } else {
            assert!(matches!(
                operation.result_blocking().unwrap(),
                qubit_operation::report::OperationOutcome::Stopped(_)
            ));
        }
    }
}

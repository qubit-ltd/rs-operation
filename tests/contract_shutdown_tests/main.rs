// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use qubit_operation::context;
use qubit_operation::context::Deadline;
use qubit_operation::protocol;
use qubit_operation::report;
use qubit_operation::report::ClockDomainId;
use qubit_operation::report::MonotonicTime;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::resource;
use qubit_operation::scope;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;

use self::support::clock as manual_clock;
use self::support::gate;

mod support;

struct GateCleanup {
    gate: gate::Gate,
    started: std::sync::Arc<AtomicBool>,
}

struct RecoveryCleanup {
    id: Arc<std::sync::Mutex<Option<report::ResourceId>>>,
    registry: resource::RecoveryRegistry,
    typed_key: Arc<std::sync::Mutex<Option<report::RecoveryKey<String>>>>,
}

impl protocol::OwnedCleanup for RecoveryCleanup {
    fn begin(
        self: Box<Self>,
        _: protocol::CleanupContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = report::CleanupOutcome> + Send + 'static>> {
        let id = self.id.lock().unwrap().expect("resource ID set before cleanup");
        let (typed_key, owner) = self
            .registry
            .register_with_capsule(id, String::from("capsule"), String::from("failure owner"))
            .unwrap();
        *self.typed_key.lock().unwrap() = Some(typed_key);
        let failure = report::CleanupFailure {
            resource_id: id,
            stage: report::CleanupStage::Removing,
            summary: report::DiagnosticSummary::new("recovery owner retained"),
            source: None,
        };
        Box::pin(std::future::ready(report::CleanupOutcome::FailedWithRecovery(
            failure, owner,
        )))
    }
}

struct CountCleanup(Arc<std::sync::atomic::AtomicUsize>);

impl protocol::OwnedCleanup for CountCleanup {
    fn begin(
        self: Box<Self>,
        _: protocol::CleanupContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = report::CleanupOutcome> + Send>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::ready(report::CleanupOutcome::Cleaned))
    }
}

struct ReadyControl(report::OperationId);
impl protocol::ManagedOperationControl for ReadyControl {
    fn request_stop(&self, _: report::StopReason) {}
    fn snapshot(&self) -> report::ChildSnapshot {
        report::ChildSnapshot {
            operation_id: self.0,
            phase: report::OperationPhase::ResultReady,
            resource_phase: report::ResourcePhase::Released,
            stop_requested: false,
            released: true,
        }
    }
    fn poll_business(&self, _: &mut Context<'_>) -> Poll<report::ChildBusinessSummary> {
        Poll::Ready(report::ChildBusinessSummary {
            operation_id: self.0,
            outcome: report::ChildBusinessOutcome::Succeeded,
            summary: report::DiagnosticSummary::new("ready child"),
        })
    }
    fn poll_released(&self, _: &mut Context<'_>) -> Poll<report::ChildSettlement> {
        Poll::Ready(report::ChildSettlement {
            operation_id: self.0,
            released: true,
            summary: report::DiagnosticSummary::new("released child"),
        })
    }
}
impl protocol::OwnedCleanup for GateCleanup {
    fn begin(
        self: Box<Self>,
        _: protocol::CleanupContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = report::CleanupOutcome> + Send + 'static>> {
        Box::pin(async move {
            self.started.store(true, Ordering::SeqCst);
            self.gate.wait().await;
            report::CleanupOutcome::Cleaned
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

struct WakerProbe {
    wakes: AtomicUsize,
}

impl Wake for WakerProbe {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
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
fn test_service_group_drain_does_not_close_shared_supervisor() {
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
    other_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
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
        report::OperationOutcome::Stopped(_)
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
        report::OperationOutcome::Succeeded(())
    ));
}

#[test]
fn test_global_pending_request_count_sums_independent_service_groups_without_root_double_count() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let first = supervisor.service_group(2).unwrap();
    let second = supervisor.service_group(2).unwrap();
    let request_a = first.try_admit().unwrap();
    let request_b = second.try_admit().unwrap();
    let report = supervisor.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_millis(1));
    assert_eq!(report.pending_requests, 2);
    drop(request_a);
    drop(request_b);
}

#[test]
fn test_sequential_requests_do_not_consume_permanent_root_capacity() {
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
            report::OperationOutcome::Succeeded(())
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
fn test_drain_timeout_keeps_work_alive_then_cancel_upgrade_waits_for_release() {
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
    started_rx.recv_timeout(std::time::Duration::from_secs(2)).unwrap();
    gate.wait_started();

    let pending = supervisor.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_millis(1));
    assert_eq!(pending.pending_requests, 1);
    let pending = supervisor.shutdown_blocking(ShutdownMode::Cancel, std::time::Duration::from_millis(1));
    assert_eq!(pending.pending_requests, 1);
    gate.release();
    let complete = supervisor.shutdown_blocking(ShutdownMode::Cancel, std::time::Duration::from_secs(2));
    assert!(complete.is_complete());
    assert_eq!(
        handle.observer().released_blocking().unwrap().resource_phase,
        report::ResourcePhase::Released
    );
    assert!(matches!(
        handle.result_blocking().unwrap(),
        report::OperationOutcome::Stopped(_)
    ));
    drop(handle);
}

#[test]
fn test_async_shutdown_returns_reusable_observer_after_timeout() {
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
    let clock = std::sync::Arc::new(manual_clock::ManualClock::new(ClockDomainId(907)));
    let timer_clock = clock.clone();
    let timer = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(10));
        let _ = timer_clock.advance_by(1);
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
    let complete = block_on(
        pending
            .observer
            .wait_async(Deadline::at(MonotonicTime::new(ClockDomainId(907), 2)), clock),
    )
    .unwrap();
    assert!(complete.report.is_complete());
    assert!(matches!(
        handle.observer().released_blocking().unwrap().resource_phase,
        report::ResourcePhase::Released
    ));
}

#[test]
fn test_timed_out_shutdown_observer_unregisters_its_waker() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let gate = gate::Gate::new();
    let body_gate = gate.clone();
    let mut operation = supervisor
        .start::<_, _, (), std::convert::Infallible>(ScopeOptions::default(), move |_| async move {
            body_gate.wait().await;
            Ok(())
        })
        .unwrap();
    gate.wait_started();
    supervisor.close();

    let clock = Arc::new(manual_clock::ManualClock::new(ClockDomainId(908)));
    let wake_owner = Arc::new(WakerProbe {
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&wake_owner));
    let mut context = Context::from_waker(&waker);
    let observer = supervisor.shutdown_observer();
    let mut wait =
        Box::pin(observer.wait_async(Deadline::at(MonotonicTime::new(ClockDomainId(908), 1)), clock.clone()));
    assert!(wait.as_mut().poll(&mut context).is_pending());
    let _ = clock.advance_by(1);
    assert!(matches!(wait.as_mut().poll(&mut context), Poll::Ready(Ok(_))));
    drop(wait);
    assert_eq!(Arc::strong_count(&wake_owner), 2);

    gate.release();
    let _ = operation.result_blocking().unwrap();
    let _ = operation.released_blocking().unwrap();
}

#[test]
fn test_shutdown_observer_waiters_respect_finite_capacity() {
    let supervisor = OperationSupervisor::new(SupervisorLimits {
        admission_waiters: 1,
        ..SupervisorLimits::default()
    });
    let gate = gate::Gate::new();
    let body_gate = gate.clone();
    let mut operation = supervisor
        .start::<_, _, (), std::convert::Infallible>(ScopeOptions::default(), move |_| async move {
            body_gate.wait().await;
            Ok(())
        })
        .unwrap();
    gate.wait_started();
    supervisor.close();
    let clock = Arc::new(manual_clock::ManualClock::new(ClockDomainId(909)));
    let deadline = Deadline::at(MonotonicTime::new(ClockDomainId(909), 10));
    let observer = supervisor.shutdown_observer();
    let first_waker = Waker::noop();
    let mut first_context = Context::from_waker(first_waker);
    let mut first_wait = Box::pin(observer.wait_async(deadline, clock.clone()));
    assert!(first_wait.as_mut().poll(&mut first_context).is_pending());

    let second_waker = Waker::noop();
    let mut second_context = Context::from_waker(second_waker);
    let mut second_wait = Box::pin(observer.wait_async(deadline, clock));
    assert!(matches!(
        second_wait.as_mut().poll(&mut second_context),
        Poll::Ready(Err(context::ContextError::WaiterCapacityExceeded))
    ));

    drop(first_wait);
    gate.release();
    let _ = operation.result_blocking().unwrap();
    let _ = operation.released_blocking().unwrap();
}

#[test]
fn test_dropping_last_public_supervisor_clone_cancels_but_observer_does_not_keep_it_public() {
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
        report::OperationOutcome::Stopped(_)
    ));
    assert_eq!(
        observer.released_blocking().unwrap().resource_phase,
        report::ResourcePhase::Released
    );
}

#[test]
fn test_service_resource_scope_has_independent_quota_and_cleanup_is_kept_alive() {
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
    assert_eq!(rejected.reason, report::AdmissionError::ReservationLimit);
    drop(rejected.into_owner());
    assert_eq!(supervisor.active_roots(), 0);

    let pending = group.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_millis(10));
    assert_eq!(pending.pending_resources, 1);
    assert!(started.load(Ordering::SeqCst));
    assert!(matches!(
        scope.acquire_use(id),
        Err(resource::ResourceError::ClosedForUse)
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
fn test_global_cancel_closes_service_groups_and_waits_for_service_resource_cleanup() {
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

    let pending = supervisor.shutdown_blocking(ShutdownMode::Cancel, std::time::Duration::from_millis(10));
    assert_eq!(pending.pending_resources, 1);
    cleanup_gate.wait_started();
    assert!(started.load(Ordering::SeqCst));
    assert!(matches!(group.try_admit(), Err(report::AdmissionError::Closed)));
    cleanup_gate.release();
    cleanup_gate.wait_released();
    assert!(
        supervisor
            .shutdown_blocking(ShutdownMode::Cancel, std::time::Duration::from_secs(1))
            .is_complete()
    );
}

#[test]
fn test_unresolved_recovery_owner_is_reported_as_shutdown_responsibility() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let group = supervisor.service_group(2).unwrap();
    let id_slot = Arc::new(std::sync::Mutex::new(None));
    let cleanup_registry = resource::RecoveryRegistry::new(1);
    let scope_slot = Arc::new(std::sync::Mutex::new(None));
    let id_for_body = Arc::clone(&id_slot);
    let scope_for_body = Arc::clone(&scope_slot);
    let registry_for_cleanup = cleanup_registry.clone();
    let typed_key_slot = Arc::new(std::sync::Mutex::new(None));
    let typed_key_for_cleanup = Arc::clone(&typed_key_slot);
    let mut operation = group
        .start(ScopeOptions::default(), move |scope| async move {
            *scope_for_body.lock().unwrap() = Some(scope.clone());
            let id = scope
                .register_resource(RecoveryCleanup {
                    id: Arc::clone(&id_for_body),
                    registry: registry_for_cleanup,
                    typed_key: typed_key_for_cleanup,
                })
                .unwrap();
            *id_for_body.lock().unwrap() = Some(id);
            Ok::<_, std::convert::Infallible>(())
        })
        .unwrap();
    assert!(matches!(
        operation.result_blocking().unwrap(),
        report::OperationOutcome::Succeeded(())
    ));
    let settlement = operation.released_blocking().unwrap();
    assert_eq!(settlement.resource_phase, report::ResourcePhase::RecoveryRequired);
    let report = supervisor.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(1));
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
fn test_dropping_service_group_retains_and_runs_service_resource_cleanup() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let group = supervisor.service_group(2).unwrap();
    let resource_scope = group.resource_scope(1).unwrap();
    let cleanup_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    resource_scope
        .register_resource(CountCleanup(Arc::clone(&cleanup_calls)))
        .unwrap();

    drop(resource_scope);
    drop(group);

    let report = supervisor.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(1));
    assert!(report.is_complete(), "dropped service group must settle: {report:?}");
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn test_service_resource_scope_can_extract_its_typed_recovery_owner() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let group = supervisor.service_group(2).unwrap();
    let resource_scope = group.resource_scope(1).unwrap();
    let id_slot = Arc::new(std::sync::Mutex::new(None));
    let typed_key_slot = Arc::new(std::sync::Mutex::new(None));
    let cleanup_registry = resource::RecoveryRegistry::new(1);
    let id = resource_scope
        .register_resource(RecoveryCleanup {
            id: Arc::clone(&id_slot),
            registry: cleanup_registry,
            typed_key: Arc::clone(&typed_key_slot),
        })
        .unwrap();
    *id_slot.lock().unwrap() = Some(id);
    let report = group.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(1));
    assert_eq!(report.pending_recovery, 1);
    let outer_key = resource_scope.take_recovery_key().unwrap();
    let recovery_owner = resource_scope.take_recovery(outer_key).unwrap();
    let typed_key = typed_key_slot.lock().unwrap().take().unwrap();
    assert_eq!(recovery_owner.take(typed_key).unwrap(), "failure owner");
    assert!(
        supervisor
            .shutdown_observer()
            .wait_blocking(std::time::Duration::from_secs(1))
            .report
            .is_complete()
    );
}

#[test]
fn test_drain_allows_existing_root_to_commit_children_but_cancel_rejects_commit() {
    for (mode, expected_commit) in [(ShutdownMode::Drain, true), (ShutdownMode::Cancel, false)] {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let gate = gate::Gate::new();
        let body_gate = gate.clone();
        let (commit_tx, commit_rx) = mpsc::channel();
        let mut operation = supervisor
            .start::<_, _, (), ()>(ScopeOptions::default(), move |scope| async move {
                body_gate.wait().await;
                let mut reservation = scope.reserve_child(scope::ChildRequirement::Required).unwrap();
                reservation
                    .attach(Arc::new(ReadyControl(report::OperationId::new(900_000))))
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
            commit_rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap(),
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
                report::OperationOutcome::Succeeded(())
            ));
        } else {
            assert!(matches!(
                operation.result_blocking().unwrap(),
                report::OperationOutcome::Stopped(_)
            ));
        }
    }
}

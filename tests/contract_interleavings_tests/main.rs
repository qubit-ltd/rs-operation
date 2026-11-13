// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod support;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use qubit_operation::driver::PortableDriver;
use qubit_operation::driver::PortableDriverConfig;
use qubit_operation::protocol::CleanupContext;
use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::protocol::OwnedCleanup;
use qubit_operation::report::ChildBusinessOutcome;
use qubit_operation::report::ChildBusinessSummary;
use qubit_operation::report::ChildSettlement;
use qubit_operation::report::ChildSnapshot;
use qubit_operation::report::CleanupOutcome;
use qubit_operation::report::ClockDomainId;
use qubit_operation::report::DiagnosticSummary;
use qubit_operation::report::MonotonicTime;
use qubit_operation::report::OperationId;
use qubit_operation::report::OperationPhase;
use qubit_operation::report::ResourcePhase;
use qubit_operation::report::StopReason;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ChildRequirement;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;

use self::support::clock;
use self::support::gate::Gate;

#[derive(Default)]
struct ControlState {
    stop_requests: usize,
    released: bool,
}

struct ControlledChild {
    id: OperationId,
    state: Arc<std::sync::Mutex<ControlState>>,
}

impl ManagedOperationControl for ControlledChild {
    fn request_stop(&self, _: StopReason) {
        self.state.lock().expect("state lock").stop_requests += 1;
    }

    fn snapshot(&self) -> ChildSnapshot {
        let state = self.state.lock().expect("state lock");
        ChildSnapshot {
            operation_id: self.id,
            phase: if state.released {
                OperationPhase::ResultReady
            } else {
                OperationPhase::Running
            },
            resource_phase: if state.released {
                ResourcePhase::Released
            } else if state.stop_requests > 0 {
                ResourcePhase::Stopping
            } else {
                ResourcePhase::Active
            },
            stop_requested: state.stop_requests > 0,
            released: state.released,
        }
    }

    fn poll_business(&self, _: &mut Context<'_>) -> Poll<ChildBusinessSummary> {
        Poll::Ready(ChildBusinessSummary {
            operation_id: self.id,
            outcome: ChildBusinessOutcome::Succeeded,
            summary: DiagnosticSummary::new("child complete"),
        })
    }

    fn poll_released(&self, _: &mut Context<'_>) -> Poll<ChildSettlement> {
        let state = self.state.lock().expect("state lock");
        if state.released {
            Poll::Ready(ChildSettlement {
                operation_id: self.id,
                released: true,
                summary: DiagnosticSummary::new("fake child released"),
            })
        } else {
            Poll::Pending
        }
    }
}

impl ControlledChild {
    fn stop_requests(&self) -> usize {
        self.state.lock().expect("state lock").stop_requests
    }

    fn confirm_release(&self) {
        self.state.lock().expect("state lock").released = true;
    }
}

struct FacadeControl(Arc<dyn ManagedOperationControl>);

impl ManagedOperationControl for FacadeControl {
    fn request_stop(&self, reason: StopReason) {
        self.0.request_stop(reason);
    }

    fn snapshot(&self) -> ChildSnapshot {
        self.0.snapshot()
    }

    fn poll_business(&self, cx: &mut Context<'_>) -> Poll<ChildBusinessSummary> {
        self.0.poll_business(cx)
    }

    fn poll_released(&self, cx: &mut Context<'_>) -> Poll<ChildSettlement> {
        self.0.poll_released(cx)
    }
}

struct FacadeCleanup(Arc<AtomicUsize>);

impl OwnedCleanup for FacadeCleanup {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { CleanupOutcome::Cleaned })
    }
}

#[test]
fn test_cancellation_winning_before_commit_prevents_child_registration() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let state = Arc::new(std::sync::Mutex::new(ControlState::default()));
    let body_state = Arc::clone(&state);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let mut reservation = scope
                .reserve_child(ChildRequirement::Required)
                .expect("reservation succeeds before cancellation");
            reservation
                .attach(Arc::new(ControlledChild {
                    id: OperationId::new(1301),
                    state: Arc::clone(&body_state),
                }))
                .expect("control attaches while suspended");
            scope.request_cancel(StopReason::UserCancelled);
            assert!(reservation.commit().is_err(), "cancel before commit must deny start");
            assert_eq!(body_state.lock().expect("state lock").stop_requests, 0);
            Ok::<_, ()>(())
        })
        .expect("root operation starts");
    assert!(matches!(
        handle.result_blocking().expect("result available"),
        qubit_operation::report::OperationOutcome::Succeeded(())
    ));
    let _ = handle.released_blocking().expect("operation settles");
}

#[test]
fn test_cancellation_accepted_before_body_completion_is_the_stable_result() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let body_gate = Gate::new();
    let entered = body_gate.clone();
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |_| async move {
            entered.wait().await;
            Ok::<_, ()>("completed after stop")
        })
        .expect("root operation starts");
    body_gate.wait_started();
    assert!(body_gate.started());
    assert!(!body_gate.is_released());
    handle.request_cancel(StopReason::UserCancelled);
    body_gate.release();
    body_gate.wait_released();

    let outcome = handle.result_blocking().expect("operation produces a result");
    assert!(matches!(
        outcome,
        qubit_operation::report::OperationOutcome::Stopped(ref stop)
            if stop.reason == StopReason::UserCancelled
    ));
    let first = handle.released_blocking().expect("operation settles");
    handle.request_cancel(StopReason::Timeout);
    assert_eq!(handle.released_blocking().expect("settlement repeats"), first);
}

struct ThreadWake {
    thread: std::thread::Thread,
    wakes: AtomicUsize,
}

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.thread.unpark();
    }
}

#[test]
fn test_result_waiter_registered_before_completion_is_woken_once_result_is_published() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let body_gate = Gate::new();
    let body_gate_task = body_gate.clone();
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |_| async move {
            body_gate_task.wait().await;
            Ok::<_, ()>("complete")
        })
        .expect("root operation starts");
    body_gate.wait_started();
    let waiter = Arc::new(ThreadWake {
        thread: std::thread::current(),
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&waiter));
    let mut cx = Context::from_waker(&waker);
    let mut result = Box::pin(handle.result());
    assert!(matches!(result.as_mut().poll(&mut cx), Poll::Pending));
    body_gate.release();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let outcome = loop {
        if let Poll::Ready(value) = result.as_mut().poll(&mut cx) {
            break value.expect("single result delivered");
        }
        assert!(std::time::Instant::now() < deadline, "result waiter timed out");
        std::thread::park_timeout(std::time::Duration::from_millis(20));
    };
    assert_eq!(
        outcome,
        qubit_operation::report::OperationOutcome::Succeeded("complete")
    );
    assert!(waiter.wakes.load(Ordering::SeqCst) > 0);
    drop(result);
    assert!(handle.released_blocking().expect("root settles").all_released);
}

#[test]
fn test_facade_control_wraps_one_registered_domain_owner_and_cleans_its_lease_once() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let body_gate = Gate::new();
    let body_gate_task = body_gate.clone();
    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let body_cleanup_calls = Arc::clone(&cleanup_calls);
    let inner_control = Arc::new(std::sync::Mutex::new(None));
    let body_inner_control = Arc::clone(&inner_control);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let resource = scope
                .register_resource(FacadeCleanup(body_cleanup_calls))
                .expect("facade-owned domain resource registered once");
            let use_lease = scope.acquire_use(resource).expect("wrapped operation holds lease");
            let domain_control = Arc::new(ControlledChild {
                id: OperationId::new(14_001),
                state: Arc::new(std::sync::Mutex::new(ControlState::default())),
            });
            let mut reservation = scope
                .reserve_child(ChildRequirement::Required)
                .expect("one facade registration slot available");
            reservation
                .attach_with_uses(Arc::new(FacadeControl(domain_control.clone())), vec![use_lease])
                .expect("facade forwards one suspended domain control and its lease");
            reservation.commit().expect("facade child committed once");
            *body_inner_control.lock().expect("control slot lock") = Some(domain_control);
            body_gate_task.wait().await;
            Ok::<_, ()>(())
        })
        .expect("root operation starts");
    body_gate.wait_started();
    handle.request_cancel(StopReason::UserCancelled);
    let control = inner_control
        .lock()
        .expect("control slot lock")
        .clone()
        .expect("wrapped control registered before gate");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while control.stop_requests() == 0 {
        assert!(std::time::Instant::now() < deadline, "facade did not forward stop");
        std::thread::yield_now();
    }
    assert_eq!(control.stop_requests(), 1);
    control.confirm_release();
    body_gate.release();

    assert!(matches!(
        handle.result_blocking().expect("facade operation stops"),
        qubit_operation::report::OperationOutcome::Stopped(_)
    ));
    assert!(handle.released_blocking().expect("facade lease settles").all_released);
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn test_body_panic_is_reported_without_skipping_root_settlement() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let (panic_tx, panic_rx) = std::sync::mpsc::channel();
    let mut handle = supervisor
        .start::<_, _, (), ()>(ScopeOptions::default(), move |_| async move {
            panic_tx.send(()).expect("test observes body entry");
            panic!("fake domain body panic");
        })
        .expect("root operation starts");
    panic_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("body entered before shutdown observation");
    let report = supervisor.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_millis(20));
    assert_eq!(
        report.pending_requests, 0,
        "body panic must be converted into terminal operation state"
    );
    let mut result = Box::pin(handle.result());
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(matches!(
        result.as_mut().poll(&mut cx),
        Poll::Ready(Ok(qubit_operation::report::OperationOutcome::Panicked(_)))
    ));
}

struct ServiceOwner {
    cleanup_calls: Arc<AtomicUsize>,
    owner_drops: Arc<AtomicUsize>,
}

impl OwnedCleanup for ServiceOwner {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { CleanupOutcome::Cleaned })
    }
}

impl Drop for ServiceOwner {
    fn drop(&mut self) {
        self.owner_drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_dropping_unclosed_service_group_does_not_drop_cleanup_owner_without_cleanup() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let group = supervisor.service_group(1).expect("service group available");
    let scope = group.resource_scope(1).expect("resource scope available");
    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let owner_drops = Arc::new(AtomicUsize::new(0));
    scope
        .register_resource(ServiceOwner {
            cleanup_calls: Arc::clone(&cleanup_calls),
            owner_drops: Arc::clone(&owner_drops),
        })
        .expect("service owner registered");

    drop(scope);
    drop(group);
    let report = supervisor.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_secs(2));

    assert!(report.is_complete());
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
    assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
}

struct WakerProbe {
    wakes: AtomicUsize,
}

impl Wake for WakerProbe {
    fn wake(self: Arc<Self>) {
        self.wakes.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn test_shutdown_observer_timeout_drops_its_registered_waker() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let body_gate = Gate::new();
    let task_gate = body_gate.clone();
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |_| async move {
            task_gate.wait().await;
            Ok::<_, ()>(())
        })
        .expect("active root starts");
    body_gate.wait_started();
    supervisor.close();
    let observer = supervisor.shutdown_observer();
    let clock = Arc::new(clock::ManualClock::new(ClockDomainId(77)));
    let wake_owner = Arc::new(WakerProbe {
        wakes: AtomicUsize::new(0),
    });
    let waker = Waker::from(Arc::clone(&wake_owner));
    let mut cx = Context::from_waker(&waker);
    let deadline = qubit_operation::context::Deadline::at(MonotonicTime::new(ClockDomainId(77), 1));
    let mut wait = Box::pin(observer.wait_async(deadline, clock.clone()));
    assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Pending));
    assert_eq!(clock.pending_timers(), 1);

    let _ = clock.advance_by(1);
    assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Ready(Ok(_))));
    assert_eq!(clock.pending_timers(), 0);
    drop(wait);
    assert_eq!(
        Arc::strong_count(&wake_owner),
        2,
        "timed-out observer must unregister its shutdown waker"
    );
    body_gate.release();
    let _ = handle.result_blocking().expect("root completes after gate release");
    let _ = handle.released_blocking().expect("root settles");
}

#[test]
fn test_portable_driver_keeps_serving_after_a_future_panics() {
    let driver = PortableDriver::new(PortableDriverConfig {
        worker_count: 1,
        task_capacity: 2,
        polls_per_turn: 1,
    })
    .expect("portable driver starts");
    let (panic_tx, panic_rx) = std::sync::mpsc::channel();
    driver
        .spawn(async move {
            panic_tx.send(()).expect("test observes panic task entry");
            panic!("fake driver task panic");
        })
        .expect("panic task accepted");
    panic_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("panic task entered");

    let (next_tx, next_rx) = std::sync::mpsc::channel();
    driver
        .spawn(async move {
            next_tx.send(()).expect("next task completes");
        })
        .expect("driver still accepts tasks after a task panic");
    assert!(
        next_rx.recv_timeout(std::time::Duration::from_millis(50)).is_ok(),
        "worker continues polling after one task panics"
    );
}

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
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
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_operation::context::ContextOptions;
use qubit_operation::context::Deadline;
use qubit_operation::protocol::CleanupContext;
use qubit_operation::protocol::OwnedCleanup;
use qubit_operation::report::CleanupOutcome;
use qubit_operation::report::ClockDomainId;
use qubit_operation::report::DiagnosticSummary;
use qubit_operation::report::EffectFact;
use qubit_operation::report::EffectRecord;
use qubit_operation::report::MonotonicTime;
use qubit_operation::report::OperationOutcome;
use qubit_operation::report::ResourcePhase;
use qubit_operation::report::StopReason;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ChildRequirement;
use qubit_operation::scope::ScopeError;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;

use self::support::BusinessResult;
use self::support::FakeControl;
use self::support::Gate;
use self::support::ManualClock;

struct FakeInputCleanup(Arc<AtomicUsize>);

impl OwnedCleanup for FakeInputCleanup {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { CleanupOutcome::Cleaned })
    }
}

struct PanicCleanup;

impl OwnedCleanup for PanicCleanup {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        Box::pin(PanicCleanupFuture)
    }
}

struct PanicCleanupFuture;

impl Future for PanicCleanupFuture {
    type Output = CleanupOutcome;

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        panic!("fake upload cleanup panic")
    }
}

struct GateCleanup {
    gate: Gate,
    calls: Arc<AtomicUsize>,
}

impl OwnedCleanup for GateCleanup {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            self.gate.wait().await;
            CleanupOutcome::Cleaned
        })
    }
}

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    #[inline]
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    #[inline]
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}

#[test]
fn test_cancelled_pipeline_keeps_input_owned_until_the_helper_exits_then_cleans_once() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let body_cleanup_calls = Arc::clone(&cleanup_calls);
    let helper_gate = Gate::new();
    let helper_started = helper_gate.clone();
    let mut operation = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let input = scope
                .register_resource(FakeInputCleanup(body_cleanup_calls))
                .expect("fake input owner registered before helper start");
            let input_use = scope.acquire_use(input).expect("helper leases its input");

            // Fake download → stage → convert → upload helper remains in flight here.
            helper_started.wait().await;
            drop(input_use);
            Ok::<_, &'static str>("uploaded")
        })
        .expect("pipeline root is accepted");

    helper_gate.wait_started();
    assert!(helper_gate.started());
    assert!(!helper_gate.is_released());
    operation.request_cancel(StopReason::UserCancelled);
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);
    helper_gate.release();
    helper_gate.wait_released();

    assert!(matches!(
        operation.result_blocking().expect("cancelled result is observable"),
        OperationOutcome::Stopped(ref stop) if stop.reason == StopReason::UserCancelled
    ));
    let settlement = operation.released_blocking().expect("helper and cleanup settle");
    assert_eq!(settlement.resource_phase, ResourcePhase::Released);
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn test_upload_error_remains_primary_when_cleanup_panic_leaves_release_unconfirmed() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let mut operation = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            scope
                .register_resource(PanicCleanup)
                .expect("fake upload owner registered before upload");
            Err::<(), _>("upload rejected")
        })
        .expect("pipeline root is accepted");

    assert!(matches!(
        operation.result_blocking().expect("business result remains observable"),
        OperationOutcome::Failed("upload rejected")
    ));
    let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_millis(20));
    assert_eq!(
        shutdown.pending_requests, 0,
        "cleanup panic must settle without leaving a root operation active"
    );
    let settlement = operation
        .released_blocking()
        .expect("cleanup panic has a stable settlement snapshot");
    assert_eq!(settlement.resource_phase, ResourcePhase::Unconfirmed);
    assert_eq!(settlement.unconfirmed_count, 1);
    assert!(!settlement.all_released);
}

#[test]
fn test_pipeline_records_ordered_stages_and_cancel_at_each_boundary() {
    for cancel_at in [Some(0), Some(1), Some(2), Some(3), None] {
        let supervisor = OperationSupervisor::new(SupervisorLimits::default());
        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        let body_cleanup_calls = Arc::clone(&cleanup_calls);
        let gates = (0..4).map(|_| Gate::new()).collect::<Vec<_>>();
        let body_gates = gates.clone();
        let (stage_tx, stage_rx) = mpsc::channel();
        let mut operation = supervisor
            .start(ScopeOptions::default(), move |scope| async move {
                let input = scope
                    .register_resource(FakeInputCleanup(body_cleanup_calls))
                    .expect("file owner registered before download");
                let input_use = scope.acquire_use(input).expect("pipeline helper leases input");
                let stages = ["download", "stage", "convert", "upload"];
                for (index, (stage, gate)) in stages.into_iter().zip(body_gates).enumerate() {
                    let mut reservation = scope
                        .reserve_child(ChildRequirement::Required)
                        .expect("stage has a reserved child slot");
                    let control = FakeControl::new();
                    reservation
                        .attach(Arc::new(control.clone()))
                        .expect("fake command/upload control attached before start");
                    if let Err(error) = reservation.commit() {
                        assert_eq!(
                            error,
                            ScopeError::Cancelled,
                            "cancellation prevents committing the next stage"
                        );
                        break;
                    }
                    stage_tx
                        .send((index, stage, control))
                        .expect("test observes each committed fake pipeline stage");
                    gate.wait().await;
                }
                drop(input_use);
                Ok::<_, &'static str>("published")
            })
            .expect("pipeline root accepted");

        let expected_last_stage = cancel_at.unwrap_or(3);
        let mut observed = Vec::new();
        for (index, gate) in gates.iter().enumerate().take(expected_last_stage + 1) {
            let (actual_index, stage_name, control) = stage_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("next committed domain stage starts");
            assert_eq!(actual_index, index);
            assert_eq!(stage_name, ["download", "stage", "convert", "upload"][index]);
            assert_ne!(control.operation_id().0, 0);
            gate.wait_started();
            observed.push(index);
            if cancel_at == Some(index) {
                operation.request_cancel(StopReason::UserCancelled);
                let deadline = Instant::now() + Duration::from_secs(2);
                while control.stop_requests() == 0 {
                    assert!(Instant::now() < deadline, "child stop was not delivered");
                    thread::yield_now();
                }
                assert_eq!(control.first_stop_reason(), Some(StopReason::UserCancelled));
                assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);
            } else {
                assert_eq!(control.first_stop_reason(), None);
            }
            control.complete_business(BusinessResult::Succeeded);
            control.confirm_release();
            gate.release();
        }
        assert_eq!(observed, (0..=expected_last_stage).collect::<Vec<_>>());
        let outcome = operation.result_blocking().expect("pipeline result available");
        match cancel_at {
            Some(_) => assert!(matches!(
                outcome,
                OperationOutcome::Stopped(ref stop)
                    if stop.reason == StopReason::UserCancelled
            )),
            None => assert_eq!(outcome, OperationOutcome::Succeeded("published")),
        }
        if cancel_at.is_some() {
            assert!(
                stage_rx.try_recv().is_err(),
                "no later stage commits after cancellation"
            );
        }
        assert!(operation.released_blocking().expect("pipeline settles").all_released);
        assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn test_required_upload_child_failure_is_published_as_child_failed() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let mut operation = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let mut reservation = scope
                .reserve_child(ChildRequirement::Required)
                .expect("upload child slot available");
            let control = FakeControl::new();
            reservation
                .attach(Arc::new(control.clone()))
                .expect("upload control attached before start");
            reservation.commit().expect("upload child committed");
            control.complete_business(BusinessResult::Failed);
            control.confirm_release();
            Ok::<_, &'static str>(())
        })
        .expect("pipeline root accepted");

    assert!(matches!(
        operation.result_blocking().expect("child failure is observable"),
        OperationOutcome::ChildFailed(_)
    ));
    assert!(
        operation
            .released_blocking()
            .expect("failed upload settles")
            .all_released
    );
}

#[test]
fn test_deadline_snapshot_keeps_observation_live_until_cleanup_is_confirmed() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let manual_clock = Arc::new(ManualClock::new(ClockDomainId(78)));
    let operation_clock = Arc::clone(&manual_clock);
    let cleanup_gate = Gate::new();
    let cleanup_started = cleanup_gate.clone();
    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let body_cleanup_calls = Arc::clone(&cleanup_calls);
    let mut operation = supervisor
        .start(
            ScopeOptions {
                context: ContextOptions {
                    clock: Some(operation_clock),
                    ..Default::default()
                },
                ..Default::default()
            },
            move |scope| async move {
                scope
                    .register_resource(GateCleanup {
                        gate: cleanup_gate,
                        calls: body_cleanup_calls,
                    })
                    .expect("input owner registered");
                Ok::<_, ()>("business result")
            },
        )
        .expect("root accepted");
    assert_eq!(
        operation.result_blocking().expect("business result is ready"),
        OperationOutcome::Succeeded("business result")
    );
    cleanup_started.wait_started();
    let observer = operation.observer();
    let timer_clock = Arc::clone(&manual_clock);
    let timer = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(2);
        while timer_clock.pending_timers() == 0 {
            assert!(Instant::now() < deadline, "observer timer was not registered");
            thread::yield_now();
        }
        let _ = timer_clock.advance_by(1);
    });
    let snapshot = block_on(observer.observe_until(Deadline::at(MonotonicTime::new(ClockDomainId(78), 1))))
        .expect("deadline observation succeeds")
        .snapshot;
    timer.join().expect("manual clock timer completed");
    assert!(snapshot.settlement.is_none());
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);

    cleanup_started.release();
    cleanup_started.wait_released();
    assert!(
        observer
            .released_blocking()
            .expect("same observer continues")
            .all_released
    );
}

#[test]
fn test_upload_effect_record_survives_cancellation_in_settlement() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let upload_gate = Gate::new();
    let body_gate = upload_gate.clone();
    let mut operation = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            scope
                .record_effect(EffectRecord {
                    operation_id: scope.operation_id(),
                    resource_id: None,
                    description: DiagnosticSummary::new("fake upload committed"),
                    fact: EffectFact::Known,
                })
                .expect("known upload effect fits report capacity");
            scope
                .record_effect(EffectRecord {
                    operation_id: scope.operation_id(),
                    resource_id: None,
                    description: DiagnosticSummary::new("remote upload acknowledgement lost"),
                    fact: EffectFact::Unknown,
                })
                .expect("uncertain remote effect fits report capacity");
            body_gate.wait().await;
            Ok::<_, &'static str>(())
        })
        .expect("pipeline root accepted");

    upload_gate.wait_started();
    operation.request_cancel(StopReason::UserCancelled);
    upload_gate.release();
    assert!(matches!(
        operation.result_blocking().expect("result available"),
        OperationOutcome::Stopped(ref stop) if stop.reason == StopReason::UserCancelled
    ));
    let settlement = operation.released_blocking().expect("recorded upload settles");
    assert_eq!(settlement.effects.len(), 2);
    assert_eq!(settlement.effects[0].description.as_str(), "fake upload committed");
    assert_eq!(settlement.effects[0].fact, EffectFact::Known);
    assert_eq!(settlement.effects[0].operation_id, settlement.operation_id);
    assert_eq!(
        settlement.effects[1].description.as_str(),
        "remote upload acknowledgement lost"
    );
    assert_eq!(settlement.effects[1].fact, EffectFact::Unknown);
    assert_eq!(settlement.effects[1].operation_id, settlement.operation_id);
    assert_eq!(settlement.effect_overflow, 0);
}

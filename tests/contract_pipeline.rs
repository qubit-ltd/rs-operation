// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================

#[path = "support/gate.rs"]
mod gate;

use gate::Gate;
use qubit_operation::protocol::{CleanupContext, OwnedCleanup};
use qubit_operation::report::{CleanupOutcome, OperationOutcome, StopReason, SupervisorLimits};
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

struct FakeInputCleanup(Arc<AtomicUsize>);

impl OwnedCleanup for FakeInputCleanup {
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { CleanupOutcome::Cleaned })
    }
}

struct PanicCleanup;

impl OwnedCleanup for PanicCleanup {
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
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

#[test]
fn cancelled_pipeline_keeps_input_owned_until_the_helper_exits_then_cleans_once() {
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
    let settlement = operation
        .released_blocking()
        .expect("helper and cleanup settle");
    assert_eq!(
        settlement.resource_phase,
        qubit_operation::report::ResourcePhase::Released
    );
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn upload_error_remains_primary_when_resource_cleanup_also_panics() {
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
        operation
            .result_blocking()
            .expect("business result remains observable"),
        OperationOutcome::Failed("upload rejected")
    ));
    let shutdown = supervisor.shutdown_blocking(
        qubit_operation::supervisor::ShutdownMode::Drain,
        std::time::Duration::from_millis(20),
    );
    assert_eq!(
        shutdown.pending_requests, 0,
        "cleanup panic must settle without leaving a root operation active"
    );
}

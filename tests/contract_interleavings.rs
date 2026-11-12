// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================

use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::protocol::{CleanupContext, OwnedCleanup};
use qubit_operation::report::CleanupOutcome;
use qubit_operation::report::{
    ChildBusinessOutcome, ChildBusinessSummary, ChildSettlement, ChildSnapshot, DiagnosticSummary,
    OperationId, OperationPhase, ResourcePhase, StopReason, SupervisorLimits,
};
use qubit_operation::scope::{ChildRequirement, ScopeOptions};
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
#[path = "support/gate.rs"]
mod gate;
use gate::Gate;

#[derive(Default)]
struct ControlState {
    stop_requests: usize,
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
        ChildSnapshot {
            operation_id: self.id,
            phase: OperationPhase::Running,
            resource_phase: ResourcePhase::Active,
            stop_requested: self.state.lock().expect("state lock").stop_requests > 0,
            released: false,
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
        Poll::Pending
    }
}

#[test]
fn cancellation_winning_before_commit_prevents_child_registration() {
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
            assert!(
                reservation.commit().is_err(),
                "cancel before commit must deny start"
            );
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
fn cancellation_accepted_before_body_completion_is_the_stable_result() {
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

    let outcome = handle
        .result_blocking()
        .expect("operation produces a result");
    assert!(matches!(
        outcome,
        qubit_operation::report::OperationOutcome::Stopped(ref stop)
            if stop.reason == StopReason::UserCancelled
    ));
    let first = handle.released_blocking().expect("operation settles");
    handle.request_cancel(StopReason::Timeout);
    assert_eq!(
        handle.released_blocking().expect("settlement repeats"),
        first
    );
}

#[test]
fn body_panic_is_reported_without_skipping_root_settlement() {
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
    let report =
        supervisor.shutdown_blocking(ShutdownMode::Drain, std::time::Duration::from_millis(20));
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
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
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
fn dropping_unclosed_service_group_does_not_drop_cleanup_owner_without_cleanup() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let group = supervisor
        .service_group(1)
        .expect("service group available");
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

    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
    assert_eq!(owner_drops.load(Ordering::SeqCst), 1);
}

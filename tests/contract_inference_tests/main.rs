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
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use qubit_operation::protocol::CleanupContext;
use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::protocol::OwnedCleanup;
use qubit_operation::report::ChildBusinessOutcome;
use qubit_operation::report::ChildBusinessSummary;
use qubit_operation::report::ChildSettlement;
use qubit_operation::report::ChildSnapshot;
use qubit_operation::report::CleanupOutcome;
use qubit_operation::report::DiagnosticSummary;
use qubit_operation::report::OperationId;
use qubit_operation::report::OperationOutcome;
use qubit_operation::report::OperationPhase;
use qubit_operation::report::ResourcePhase;
use qubit_operation::report::StopReason;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ChildRequirement;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use support::Gate;

static NEXT_CHILD: AtomicU64 = AtomicU64::new(20_000);

#[derive(Default)]
struct ChildState {
    business: Option<ChildBusinessOutcome>,
    released: bool,
    stop_requests: usize,
    business_waiters: Vec<Waker>,
    release_waiters: Vec<Waker>,
}

struct InferenceChild {
    id: OperationId,
    state: Mutex<ChildState>,
    stop_releases: bool,
    stop_gate: Option<Gate>,
}

impl InferenceChild {
    fn new(stop_releases: bool, stop_gate: Option<Gate>) -> Self {
        Self {
            id: OperationId::new(NEXT_CHILD.fetch_add(1, Ordering::Relaxed) as u128),
            state: Mutex::new(ChildState::default()),
            stop_releases,
            stop_gate,
        }
    }

    fn confirm_native_exit(&self) {
        let waiters = {
            let mut state = self.state.lock().expect("child state lock");
            state.released = true;
            std::mem::take(&mut state.release_waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }
}

impl ManagedOperationControl for InferenceChild {
    fn request_stop(&self, _: StopReason) {
        let waiters = {
            let mut state = self.state.lock().expect("child state lock");
            state.stop_requests += 1;
            state.business = Some(ChildBusinessOutcome::Stopped);
            if self.stop_releases {
                state.released = true;
            }
            std::mem::take(&mut state.business_waiters)
        };
        if let Some(gate) = &self.stop_gate {
            gate.release();
        }
        for waiter in waiters {
            waiter.wake();
        }
    }

    fn snapshot(&self) -> ChildSnapshot {
        let state = self.state.lock().expect("child state lock");
        ChildSnapshot {
            operation_id: self.id,
            phase: if state.business.is_some() {
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

    fn poll_business(&self, cx: &mut Context<'_>) -> Poll<ChildBusinessSummary> {
        let mut state = self.state.lock().expect("child state lock");
        match state.business {
            Some(outcome) => Poll::Ready(ChildBusinessSummary {
                operation_id: self.id,
                outcome,
                summary: DiagnosticSummary::new("fake inference child stopped"),
            }),
            None => {
                if !state.business_waiters.iter().any(|w| w.will_wake(cx.waker())) {
                    state.business_waiters.push(cx.waker().clone());
                }
                Poll::Pending
            }
        }
    }

    fn poll_released(&self, cx: &mut Context<'_>) -> Poll<ChildSettlement> {
        let mut state = self.state.lock().expect("child state lock");
        if state.released {
            Poll::Ready(ChildSettlement {
                operation_id: self.id,
                released: true,
                summary: DiagnosticSummary::new("fake inference child released"),
            })
        } else {
            if !state.release_waiters.iter().any(|w| w.will_wake(cx.waker())) {
                state.release_waiters.push(cx.waker().clone());
            }
            Poll::Pending
        }
    }
}

struct FeatureCleanup(Arc<AtomicUsize>);

impl OwnedCleanup for FeatureCleanup {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { CleanupOutcome::Cleaned })
    }
}

struct ThreadWaker(std::thread::Thread);

impl std::task::Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}

#[test]
fn test_sampling_stops_but_native_feature_lease_survives_until_worker_exit() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let body_cleanup_calls = Arc::clone(&cleanup_calls);
    let sampling_gate = Gate::new();
    let body_sampling_gate = sampling_gate.clone();
    let native_gate = Gate::new();
    let worker_gate = native_gate.clone();
    let (children_tx, children_rx) = std::sync::mpsc::channel();
    let (native_exit_tx, native_exit_rx) = std::sync::mpsc::channel();
    let mut operation = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let features = scope
                .register_resource(FeatureCleanup(body_cleanup_calls))
                .expect("feature owner registered before sampling");
            let feature_use = scope
                .acquire_use(features)
                .expect("native worker receives feature lease");

            let sampling = Arc::new(InferenceChild::new(true, Some(body_sampling_gate.clone())));
            let native = Arc::new(InferenceChild::new(false, None));
            let mut sampling_reservation = scope
                .reserve_child(ChildRequirement::Required)
                .expect("sampling child slot available");
            sampling_reservation
                .attach(sampling.clone())
                .expect("sampling control attached before start");
            sampling_reservation
                .commit()
                .expect("sampling child starts under supervision");

            let mut native_reservation = scope
                .reserve_child(ChildRequirement::Required)
                .expect("native child slot available");
            native_reservation
                .attach_with_uses(native.clone(), vec![feature_use])
                .expect("native control and feature lease attach together");
            native_reservation
                .commit()
                .expect("native child starts under supervision");
            children_tx
                .send((sampling, Arc::clone(&native)))
                .expect("test observes admitted child controls");

            let native_exit = Arc::clone(&native);
            std::thread::spawn(move || {
                block_on(worker_gate.wait());
                native_exit.confirm_native_exit();
                native_exit_tx.send(()).expect("test observes native exit");
            });

            // Sampling is interruptible; cancellation releases this gate and lets the body
            // return.
            body_sampling_gate.wait().await;
            Ok::<_, &'static str>("sampled")
        })
        .expect("inference root accepted");
    let (sampling, native) = children_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("both child controls committed");
    sampling_gate.wait_started();
    native_gate.wait_started();

    operation.request_cancel(StopReason::UserCancelled);
    assert!(matches!(
        operation.result_blocking().expect("business stop is published"),
        OperationOutcome::Stopped(ref stop) if stop.reason == StopReason::UserCancelled
    ));
    assert_eq!(sampling.snapshot().resource_phase, ResourcePhase::Released);
    assert_eq!(native.snapshot().resource_phase, ResourcePhase::Stopping);
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);
    assert!(native_gate.started());
    assert!(!native_gate.is_released());

    native_gate.release();
    native_gate.wait_released();
    native_exit_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("native worker exits after explicit gate release");
    let settlement = operation.released_blocking().expect("native resource settles");
    assert!(settlement.all_released);
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
}

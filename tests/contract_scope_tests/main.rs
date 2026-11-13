// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::thread;

use qubit_operation::context::ContextOptions;
use qubit_operation::context::Deadline;
use qubit_operation::driver::BlockingExecutor;
use qubit_operation::driver::ThreadExecutor;
use qubit_operation::driver::is_thread_executor_worker;
use qubit_operation::operation::OperationObserver;
use qubit_operation::protocol::CleanupContext;
use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::protocol::OwnedCleanup;
use qubit_operation::report::ChildBusinessOutcome;
use qubit_operation::report::ChildBusinessSummary;
use qubit_operation::report::ChildSettlement;
use qubit_operation::report::ChildSnapshot;
use qubit_operation::report::CleanupFailure;
use qubit_operation::report::CleanupOutcome;
use qubit_operation::report::CleanupStage;
use qubit_operation::report::ClockDomainId;
use qubit_operation::report::DiagnosticSummary;
use qubit_operation::report::EffectFact;
use qubit_operation::report::EffectRecord;
use qubit_operation::report::MonotonicTime;
use qubit_operation::report::OperationId;
use qubit_operation::report::OperationOutcome;
use qubit_operation::report::OperationPhase;
use qubit_operation::report::RecoveryKey;
use qubit_operation::report::RecoveryOwner;
use qubit_operation::report::ResourceId;
use qubit_operation::report::ResourcePhase;
use qubit_operation::report::ScopeLimits;
use qubit_operation::report::StopReason;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::resource::RecoveryRegistry;
use qubit_operation::scope::ChildRequirement;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::scope::ScopePolicy;
use qubit_operation::supervisor::OperationSupervisor;

mod support;

use crate::support::Gate;
use crate::support::ManualClock;

static NEXT_CHILD_ID: AtomicU64 = AtomicU64::new(1000);

fn supervisor() -> OperationSupervisor {
    OperationSupervisor::new(SupervisorLimits::default())
}

struct ThreadWaker(thread::Thread);
impl std::task::Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => thread::park(),
        }
    }
}

#[derive(Clone)]
struct ProbeControl {
    id: OperationId,
    outcome: ChildBusinessOutcome,
    state: Arc<(Mutex<ProbeState>, Condvar)>,
}
struct ProbeState {
    released: bool,
    stop_requests: usize,
    release_wakers: Vec<Waker>,
}

impl ProbeControl {
    fn new(outcome: ChildBusinessOutcome, released: bool) -> Self {
        Self {
            id: OperationId::new(NEXT_CHILD_ID.fetch_add(1, Ordering::Relaxed) as u128),
            outcome,
            state: Arc::new((
                Mutex::new(ProbeState {
                    released,
                    stop_requests: 0,
                    release_wakers: Vec::new(),
                }),
                Condvar::new(),
            )),
        }
    }
    fn release(&self) {
        let (lock, changed) = &*self.state;
        let waiters = {
            let mut state = lock.lock().unwrap();
            state.released = true;
            std::mem::take(&mut state.release_wakers)
        };
        changed.notify_all();
        for waker in waiters {
            waker.wake();
        }
    }
    fn wait_for_stop(&self) {
        let (lock, changed) = &*self.state;
        let state = lock.lock().unwrap();
        let (state, timeout) = changed
            .wait_timeout_while(state, std::time::Duration::from_secs(5), |s| s.stop_requests == 0)
            .unwrap();
        assert!(
            !timeout.timed_out() || state.stop_requests != 0,
            "stop was not delivered before watchdog"
        );
    }
    fn stop_requests(&self) -> usize {
        self.state.0.lock().unwrap().stop_requests
    }
    fn stopped_before_watchdog(&self) -> bool {
        let (lock, changed) = &*self.state;
        let state = lock.lock().unwrap();
        let (state, timeout) = changed
            .wait_timeout_while(state, std::time::Duration::from_secs(2), |s| s.stop_requests == 0)
            .unwrap();
        !timeout.timed_out() || state.stop_requests != 0
    }
}

impl ManagedOperationControl for ProbeControl {
    fn request_stop(&self, _: StopReason) {
        let (lock, changed) = &*self.state;
        lock.lock().unwrap().stop_requests += 1;
        changed.notify_all();
    }
    fn snapshot(&self) -> ChildSnapshot {
        let state = self.state.0.lock().unwrap();
        ChildSnapshot {
            operation_id: self.id,
            phase: OperationPhase::ResultReady,
            resource_phase: if state.released {
                ResourcePhase::Released
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
            outcome: self.outcome,
            summary: DiagnosticSummary::new("probe child outcome"),
        })
    }
    fn poll_released(&self, cx: &mut Context<'_>) -> Poll<ChildSettlement> {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().unwrap();
        if state.released {
            Poll::Ready(ChildSettlement {
                operation_id: self.id,
                released: true,
                summary: DiagnosticSummary::new("released"),
            })
        } else {
            if !state.release_wakers.iter().any(|w| w.will_wake(cx.waker())) {
                state.release_wakers.push(cx.waker().clone());
            }
            Poll::Pending
        }
    }
}

struct GateCleanup {
    gate: Gate,
    started: Arc<std::sync::atomic::AtomicBool>,
}

struct PanicCleanup(Arc<std::sync::atomic::AtomicBool>);

impl OwnedCleanup for PanicCleanup {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        self.0.store(true, Ordering::SeqCst);
        Box::pin(async { panic!("contract cleanup panic") })
    }
}
impl OwnedCleanup for GateCleanup {
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = qubit_operation::report::CleanupOutcome> + Send + 'static>> {
        Box::pin(async move {
            self.started.store(true, Ordering::SeqCst);
            self.gate.wait().await;
            qubit_operation::report::CleanupOutcome::Cleaned
        })
    }
}

struct RecoveryCleanup {
    id: Arc<Mutex<Option<ResourceId>>>,
    registry: RecoveryRegistry,
    key: Arc<Mutex<Option<RecoveryKey<String>>>>,
}
impl OwnedCleanup for RecoveryCleanup {
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = qubit_operation::report::CleanupOutcome> + Send + 'static>> {
        let id = self.id.lock().unwrap().expect("registration id set before cleanup");
        let (key, failure_owner) = self
            .registry
            .register_with_capsule(id, String::from("capsule"), String::from("failure owner"))
            .unwrap();
        *self.key.lock().unwrap() = Some(key);
        let failure = CleanupFailure {
            resource_id: id,
            stage: CleanupStage::Removing,
            summary: DiagnosticSummary::new("cleanup retained recovery owner"),
            source: None,
        };
        Box::pin(std::future::ready(
            qubit_operation::report::CleanupOutcome::FailedWithRecovery(failure, failure_owner),
        ))
    }
}

struct ImmediateCleanup;
impl OwnedCleanup for ImmediateCleanup {
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = qubit_operation::report::CleanupOutcome> + Send + 'static>> {
        Box::pin(std::future::ready(qubit_operation::report::CleanupOutcome::Cleaned))
    }
}
struct CountCleanup(Arc<std::sync::atomic::AtomicUsize>);
impl OwnedCleanup for CountCleanup {
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = qubit_operation::report::CleanupOutcome> + Send + 'static>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::ready(qubit_operation::report::CleanupOutcome::Cleaned))
    }
}
struct TransferCleanup {
    id: Arc<Mutex<Option<ResourceId>>>,
    receiver: qubit_operation::resource::ResourceRegistry,
    accepted: std::sync::mpsc::SyncSender<qubit_operation::report::TransferReceipt>,
}
impl OwnedCleanup for TransferCleanup {
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = qubit_operation::report::CleanupOutcome> + Send + 'static>> {
        let id = self.id.lock().unwrap().expect("source identity set before cleanup");
        let (_, receipt) = self
            .receiver
            .accept_cleanup_transfer(id, Box::new(ImmediateCleanup))
            .unwrap();
        self.accepted.send(receipt).unwrap();
        Box::pin(std::future::ready(
            qubit_operation::report::CleanupOutcome::Transferred(receipt),
        ))
    }
}

#[test]
fn test_handle_is_unique_observer_is_clone_and_body_can_start() {
    let supervisor = supervisor();
    fn assert_clone<T: Clone>() {}
    assert_clone::<OperationObserver<usize, std::io::Error>>();
    let mut handle = supervisor
        .start(ScopeOptions::default(), |_| async { Ok::<_, std::io::Error>(7) })
        .unwrap();
    let observer = handle.observer();
    let observer_copy = observer.clone();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Succeeded(7)
    ));
    assert!(observer_copy.released_blocking().unwrap().all_released);
}

#[test]
fn test_cancellation_before_commit_prevents_start_and_after_commit_reaches_control() {
    let supervisor = supervisor();
    let starts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let before = ProbeControl::new(ChildBusinessOutcome::Succeeded, true);
    let before_control = before.clone();
    let before_starts = Arc::clone(&starts);
    let mut first = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let mut reservation = scope.reserve_child(ChildRequirement::Required).unwrap();
            reservation.attach(Arc::new(before_control)).unwrap();
            scope.request_cancel(StopReason::UserCancelled);
            if reservation.commit().is_ok() {
                before_starts.fetch_add(1, Ordering::SeqCst);
            }
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    assert!(matches!(
        first.result_blocking().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 0);

    let gate = Gate::new();
    let body_gate = gate.clone();
    let after = ProbeControl::new(ChildBusinessOutcome::Succeeded, true);
    let after_control = after.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let mut second = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let mut reservation = scope.reserve_child(ChildRequirement::Required).unwrap();
            reservation.attach(Arc::new(after_control.clone())).unwrap();
            reservation.commit().unwrap();
            tx.send(()).unwrap();
            body_gate.wait().await;
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    gate.wait_started();
    second.request_cancel(StopReason::UserCancelled);
    after.wait_for_stop();
    assert_eq!(after.stop_requests(), 1);
    gate.release();
    assert!(matches!(
        second.result_blocking().unwrap(),
        OperationOutcome::Stopped(ref stop) if stop.reason == StopReason::UserCancelled
    ));
}

#[test]
fn test_required_failure_with_different_error_type_becomes_child_failed_optional_can_fallback() {
    let supervisor = supervisor();
    let required = ProbeControl::new(ChildBusinessOutcome::Failed, true);
    let control = required.clone();
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let mut reservation = scope.reserve_child(ChildRequirement::Required).unwrap();
            reservation.attach(Arc::new(control)).unwrap();
            reservation.commit().unwrap();
            Ok::<_, std::io::Error>(17_u32)
        })
        .unwrap();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::ChildFailed(_)
    ));

    let optional = ProbeControl::new(ChildBusinessOutcome::Failed, true);
    let control = optional.clone();
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let mut reservation = scope.reserve_child(ChildRequirement::Optional).unwrap();
            reservation.attach(Arc::new(control)).unwrap();
            reservation.commit().unwrap();
            Ok::<_, std::io::Error>(99_u32)
        })
        .unwrap();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Succeeded(99)
    ));
}

#[test]
fn test_typed_result_precedes_release_and_snapshot_is_repeatable() {
    let supervisor = supervisor();
    let control = ProbeControl::new(ChildBusinessOutcome::Succeeded, false);
    let child = control.clone();
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let mut reservation = scope.reserve_child(ChildRequirement::Required).unwrap();
            reservation.attach(Arc::new(child)).unwrap();
            reservation.commit().unwrap();
            Ok::<_, std::io::Error>(23_u32)
        })
        .unwrap();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Succeeded(23)
    ));
    assert_eq!(handle.observer().resource_phase(), ResourcePhase::Active);
    control.release();
    let first = handle.released_blocking().unwrap();
    let second = handle.released_blocking().unwrap();
    assert!(first.all_released);
    assert_eq!(first, second);
}

#[test]
fn test_resource_use_and_cleanup_gate_keep_released_pending_until_confirmation() {
    let supervisor = supervisor();
    let cleanup_gate = Gate::new();
    let cleanup_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleanup_gate_body = cleanup_gate.clone();
    let started_flag = Arc::clone(&cleanup_started);
    let child_control = ProbeControl::new(ChildBusinessOutcome::Succeeded, false);
    let child = child_control.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let resource = scope
                .register_resource(GateCleanup {
                    gate: cleanup_gate_body,
                    started: started_flag,
                })
                .unwrap();
            let use_lease = scope.acquire_use(resource).unwrap();
            let mut reservation = scope.reserve_child(ChildRequirement::Required).unwrap();
            reservation.attach_with_uses(Arc::new(child), vec![use_lease]).unwrap();
            reservation.commit().unwrap();
            tx.send(resource).unwrap();
            Ok::<_, std::io::Error>(71_u32)
        })
        .unwrap();
    let resource_id = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    let observer = handle.observer();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Succeeded(71)
    ));

    let mut cx = Context::from_waker(Waker::noop());
    let mut released = Box::pin(observer.released());
    assert!(
        Pin::as_mut(&mut released).poll(&mut cx).is_pending(),
        "child use lease still blocks resource cleanup"
    );
    assert!(!cleanup_started.load(Ordering::SeqCst));

    child_control.release();
    cleanup_gate.wait_started();
    assert!(cleanup_started.load(Ordering::SeqCst));
    assert!(
        Pin::as_mut(&mut released).poll(&mut cx).is_pending(),
        "cleanup has started but is not confirmed"
    );

    drop(released);
    cleanup_gate.release();
    let report = observer.released_blocking().unwrap();
    assert!(report.all_released);
    assert_eq!(report.resource_phase, ResourcePhase::Released);
    assert_eq!(report.operation_id, resource_id.operation);
}

#[test]
fn test_child_scope_transfer_keeps_owner_supervised_until_parent_join_confirms_cleanup() {
    let supervisor = supervisor();
    let cleanup_gate = Gate::new();
    let body_gate = cleanup_gate.clone();
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleanup_started = Arc::clone(&started);
    let child_control = ProbeControl::new(ChildBusinessOutcome::Succeeded, false);
    let child = child_control.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let (joined_tx, joined_rx) = std::sync::mpsc::sync_channel(1);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let resource = scope
                .register_resource(GateCleanup {
                    gate: body_gate,
                    started: cleanup_started,
                })
                .unwrap();
            let child_scope = scope.child_scope(ScopeOptions::default()).unwrap();
            let moved = scope.transfer_resource_to(resource, &child_scope).unwrap();
            let use_lease = child_scope.acquire_use(moved).unwrap();
            let mut reservation = child_scope.reserve_child(ChildRequirement::Required).unwrap();
            reservation.attach_with_uses(Arc::new(child), vec![use_lease]).unwrap();
            reservation.commit().unwrap();
            tx.send((resource, moved)).unwrap();
            let joined = scope.join().await;
            joined_tx.send(joined.settlements.len()).unwrap();
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    let (source, receiver) = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert_ne!(source.operation, receiver.operation);
    assert!(!started.load(Ordering::SeqCst));
    assert!(
        matches!(joined_rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)),
        "join must remain pending until destination cleanup is confirmed"
    );
    child_control.release();
    cleanup_gate.wait_started();
    assert!(started.load(Ordering::SeqCst));
    assert!(
        matches!(joined_rx.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)),
        "join must remain pending while cleanup has not confirmed"
    );
    cleanup_gate.release();
    assert_eq!(joined_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(), 1);
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    let observer = handle.observer();
    let report = observer.released_blocking().unwrap();
    assert!(report.all_released);
    assert_eq!(report.operation_id, source.operation);
}

#[test]
fn test_fail_fast_stops_active_siblings_while_collect_leaves_them_running() {
    for (policy, expect_stop) in [(ScopePolicy::FailFast, true), (ScopePolicy::Collect, false)] {
        let supervisor = supervisor();
        let failed = ProbeControl::new(ChildBusinessOutcome::Failed, true);
        let sibling = ProbeControl::new(ChildBusinessOutcome::Succeeded, false);
        let failed_child = failed.clone();
        let sibling_child = sibling.clone();
        let options = ScopeOptions {
            policy,
            ..ScopeOptions::default()
        };
        let mut handle = supervisor
            .start(options, move |scope| async move {
                let mut first = scope.reserve_child(ChildRequirement::Required).unwrap();
                first.attach(Arc::new(failed_child)).unwrap();
                first.commit().unwrap();
                let mut second = scope.reserve_child(ChildRequirement::Optional).unwrap();
                second.attach(Arc::new(sibling_child)).unwrap();
                second.commit().unwrap();
                Ok::<_, std::io::Error>(())
            })
            .unwrap();
        assert!(matches!(
            handle.result_blocking().unwrap(),
            OperationOutcome::ChildFailed(_)
        ));
        if expect_stop {
            sibling.wait_for_stop();
        }
        assert_eq!(sibling.stop_requests() > 0, expect_stop);
        sibling.release();
        assert!(handle.released_blocking().unwrap().all_released);
    }
}

#[test]
fn test_handle_drop_requests_stop_but_detach_keeps_operation_running() {
    let supervisor = supervisor();
    let gate = Gate::new();
    let body_gate = gate.clone();
    let control = ProbeControl::new(ChildBusinessOutcome::Succeeded, true);
    let child = control.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let mut reservation = scope.reserve_child(ChildRequirement::Optional).unwrap();
            reservation.attach(Arc::new(child.clone())).unwrap();
            reservation.commit().unwrap();
            tx.send(()).unwrap();
            body_gate.wait().await;
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    let observer = handle.observer();
    rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    gate.wait_started();
    drop(handle);
    control.wait_for_stop();
    gate.release();
    assert!(observer.released_blocking().unwrap().all_released);

    let gate = Gate::new();
    let body_gate = gate.clone();
    let control = ProbeControl::new(ChildBusinessOutcome::Succeeded, true);
    let child = control.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let mut reservation = scope.reserve_child(ChildRequirement::Optional).unwrap();
            reservation.attach(Arc::new(child.clone())).unwrap();
            reservation.commit().unwrap();
            tx.send(()).unwrap();
            body_gate.wait().await;
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    gate.wait_started();
    let observer = handle.detach();
    assert_eq!(control.stop_requests(), 0);
    gate.release();
    assert!(observer.released_blocking().unwrap().all_released);
}

#[test]
fn test_managed_worker_blocking_wait_returns_would_deadlock() {
    let supervisor = supervisor();
    let nested_supervisor = supervisor.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let mut outer = supervisor
        .start(ScopeOptions::default(), move |_| async move {
            let mut inner = nested_supervisor
                .start(ScopeOptions::default(), |_| async { Ok::<_, std::io::Error>(1_u8) })
                .unwrap();
            let result_wait = inner.result_blocking();
            let release_wait = inner.observer().released_blocking();
            tx.send((result_wait, release_wait)).unwrap();
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    let (result_wait, release_wait) = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert!(matches!(
        result_wait,
        Err(qubit_operation::report::ObservationError::WouldDeadlock)
    ));
    assert!(matches!(
        release_wait,
        Err(qubit_operation::report::ObservationError::WouldDeadlock)
    ));
    assert!(matches!(
        outer.result_blocking().unwrap(),
        OperationOutcome::Succeeded(())
    ));
}

#[test]
fn test_run_async_is_lazy_until_first_poll_and_run_to_completion_returns_settlement() {
    let supervisor = supervisor();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let body_calls = Arc::clone(&calls);
    let future = supervisor.run_async(ScopeOptions::default(), move |_| async move {
        body_calls.fetch_add(1, Ordering::SeqCst);
        Ok::<_, std::io::Error>(31_u32)
    });
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "constructing run_async must not submit the body"
    );
    assert!(matches!(block_on(future).unwrap(), OperationOutcome::Succeeded(31)));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let cleanup_gate = Gate::new();
    cleanup_gate.release();
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleanup_started = Arc::clone(&started);
    let (outcome, settlement) = block_on(supervisor.run_to_completion(
        ScopeOptions::default(),
        move |scope| async move {
            let resource = scope
                .register_resource(GateCleanup {
                    gate: cleanup_gate,
                    started: cleanup_started,
                })
                .unwrap();
            let longer_lived = scope.child_scope(ScopeOptions::default()).unwrap();
            scope.transfer_resource_to(resource, &longer_lived).unwrap();
            Ok::<_, std::io::Error>(41_u32)
        },
    ))
    .unwrap();
    assert!(matches!(outcome, OperationOutcome::Succeeded(41)));
    assert!(settlement.all_released);
    assert!(started.load(Ordering::SeqCst));
}

#[test]
fn test_result_is_taken_once_and_observation_releases_reserved_result_slot() {
    let supervisor = OperationSupervisor::new(SupervisorLimits {
        active_roots: 2,
        queued_roots: 0,
        unobserved_results: 1,
        ..SupervisorLimits::default()
    });
    let mut first = supervisor
        .start(ScopeOptions::default(), |_| async { Ok::<_, std::io::Error>(1_u8) })
        .unwrap();
    assert_eq!(supervisor.reserved_results(), 1);
    let rejected = match supervisor.start(ScopeOptions::default(), |_| async { Ok::<_, std::io::Error>(2_u8) }) {
        Err(error) => error,
        Ok(_) => panic!("reserved result slot must reject another root"),
    };
    assert_eq!(rejected.reason(), qubit_operation::report::AdmissionError::Busy);
    let _ = rejected.into_body();
    assert!(matches!(
        first.result_blocking().unwrap(),
        OperationOutcome::Succeeded(1)
    ));
    assert_eq!(supervisor.reserved_results(), 0);
    assert!(matches!(
        first.result_blocking(),
        Err(qubit_operation::report::ObservationError::AlreadyTaken)
    ));
    let mut second = supervisor
        .start(ScopeOptions::default(), |_| async { Ok::<_, std::io::Error>(3_u8) })
        .unwrap();
    assert!(matches!(
        second.result_blocking().unwrap(),
        OperationOutcome::Succeeded(3)
    ));
}

#[test]
fn test_dropping_result_wait_future_keeps_handle_and_operation_live() {
    let supervisor = supervisor();
    let gate = Gate::new();
    let mut handle = supervisor
        .start(ScopeOptions::default(), {
            let gate = gate.clone();
            move |_| async move {
                gate.wait().await;
                Ok::<_, std::io::Error>(23usize)
            }
        })
        .unwrap();
    gate.wait_started();
    let mut result_wait = Box::pin(handle.result());
    let waker = std::task::Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    assert!(result_wait.as_mut().poll(&mut cx).is_pending());
    drop(result_wait);
    gate.release();
    assert!(matches!(
        block_on(handle.result()).unwrap(),
        OperationOutcome::Succeeded(23)
    ));
    assert!(handle.released_blocking().unwrap().all_released);
}

#[test]
fn test_queued_root_cancel_before_body_start_has_zero_body_side_effect() {
    let supervisor = OperationSupervisor::new(SupervisorLimits {
        active_roots: 0,
        queued_roots: 1,
        unobserved_results: 1,
        ..SupervisorLimits::default()
    });
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let body_calls = Arc::clone(&calls);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |_| async move {
            body_calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    handle.request_cancel(StopReason::UserCancelled);
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Stopped(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn test_queued_root_waits_for_execution_lease_promotion_before_body_start() {
    let supervisor = OperationSupervisor::new(SupervisorLimits {
        active_roots: 1,
        queued_roots: 1,
        unobserved_results: 2,
        ..SupervisorLimits::default()
    });
    let gate = Gate::new();
    let first_gate = gate.clone();
    let mut first = supervisor
        .start(ScopeOptions::default(), move |_| async move {
            first_gate.wait().await;
            Ok::<_, std::io::Error>(1_u8)
        })
        .unwrap();
    gate.wait_started();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let second_calls = Arc::clone(&calls);
    let mut second = supervisor
        .start(ScopeOptions::default(), move |_| async move {
            second_calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, std::io::Error>(2_u8)
        })
        .unwrap();
    assert_eq!(supervisor.queued_roots(), 1);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "queued body cannot run before active lease promotion"
    );
    gate.release();
    assert!(matches!(
        first.result_blocking().unwrap(),
        OperationOutcome::Succeeded(1)
    ));
    assert!(matches!(
        second.result_blocking().unwrap(),
        OperationOutcome::Succeeded(2)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn test_scope_clone_cannot_commit_after_body_close_but_committed_child_retains_authority() {
    let supervisor = supervisor();
    let control = ProbeControl::new(ChildBusinessOutcome::Succeeded, false);
    let child = control.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let clone = scope.clone();
            let mut reservation = scope.reserve_child(ChildRequirement::Required).unwrap();
            reservation.attach(Arc::new(child)).unwrap();
            reservation.commit().unwrap();
            tx.send(clone).unwrap();
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    let escaped_clone = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert!(matches!(
        escaped_clone.reserve_child(ChildRequirement::Optional),
        Err(qubit_operation::scope::ScopeError::Closed)
    ));
    assert_eq!(
        control.stop_requests(),
        0,
        "closing the body's submit right preserves already committed child work"
    );
    control.release();
    assert!(handle.released_blocking().unwrap().all_released);
}

#[test]
fn test_cleanup_recovery_is_transferred_and_report_remains_a_stable_history() {
    let supervisor = supervisor();
    let recovery = RecoveryRegistry::new(1);
    let key_slot = Arc::new(Mutex::new(None));
    let cleanup_key_slot = Arc::clone(&key_slot);
    let id_slot = Arc::new(Mutex::new(None));
    let cleanup_id_slot = Arc::clone(&id_slot);
    let (scope_tx, scope_rx) = std::sync::mpsc::sync_channel(1);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let child_scope = scope.clone();
            let resource = scope
                .register_resource(RecoveryCleanup {
                    id: cleanup_id_slot.clone(),
                    registry: recovery,
                    key: cleanup_key_slot,
                })
                .unwrap();
            *cleanup_id_slot.lock().unwrap() = Some(resource);
            scope_tx.send(child_scope).unwrap();
            Ok::<_, std::io::Error>(53_u32)
        })
        .unwrap();
    let scope = scope_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Succeeded(53)
    ));
    let observer = handle.observer();
    let report = observer.released_blocking().unwrap();
    assert_eq!(report.resource_phase, ResourcePhase::RecoveryRequired);
    assert!(
        report.all_released,
        "recovery ownership has moved into the supervisor's bounded recovery registry"
    );
    assert_eq!(report.cleanup_failures.len(), 1);
    assert_eq!(report, observer.released_blocking().unwrap());

    let resource_id = id_slot.lock().unwrap().expect("registered resource identity");
    let other_registry = RecoveryRegistry::new(1);
    let (_, foreign_owner) = other_registry
        .register_with_capsule(
            resource_id,
            String::from("foreign capsule"),
            String::from("foreign failure"),
        )
        .unwrap();
    let wrong_registry = RecoveryRegistry::new(1);
    let wrong_key = wrong_registry.register(resource_id, foreign_owner).unwrap();
    let wrong_key = scope.take_recovery(wrong_key).unwrap_err();
    assert_eq!(
        wrong_key.resource_id(),
        resource_id,
        "a foreign registry key is returned unchanged"
    );

    let outer_key = scope.take_recovery_key().expect("failed cleanup issued a one-use key");
    let recovery_owner: RecoveryOwner = scope
        .take_recovery(outer_key)
        .expect("scope recovery registry accepts its own key");
    let original_key = key_slot
        .lock()
        .unwrap()
        .take()
        .expect("adapter registry key retained for the original typed owner");
    assert_eq!(recovery_owner.take(original_key).unwrap(), "failure owner");
    assert!(
        scope.take_recovery_key().is_none(),
        "the recovery entry is extracted once"
    );
    assert_eq!(
        report,
        observer.released_blocking().unwrap(),
        "recovery extraction cannot rewrite historical settlement"
    );
}

#[test]
fn test_transferred_cleanup_requires_receiver_release_receipt_to_clear_source_tombstone() {
    let supervisor = supervisor();
    let receiver = qubit_operation::resource::ResourceRegistry::new(OperationId::new(9000), 1);
    let receiver_registry = receiver.clone();
    let id_slot = Arc::new(Mutex::new(None));
    let cleanup_id = Arc::clone(&id_slot);
    let a_cleanup_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let a_calls = Arc::clone(&a_cleanup_calls);
    let (accepted_tx, accepted_rx) = std::sync::mpsc::sync_channel(1);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let source_scope = scope.clone();
            let source_id = scope
                .register_resource(TransferCleanup {
                    id: cleanup_id.clone(),
                    receiver: receiver_registry,
                    accepted: accepted_tx,
                })
                .unwrap();
            *cleanup_id.lock().unwrap() = Some(source_id);
            let dependent = scope.register_resource(CountCleanup(a_calls)).unwrap();
            scope.cleanup_after(dependent, source_id).unwrap();
            tx.send((source_scope, dependent)).unwrap();
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    let (source_scope, _dependent_id) = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    let source_id = id_slot.lock().unwrap().expect("source registration was created");
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    let observer = handle.observer();
    let transfer = accepted_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert_eq!(transfer.resource(), source_id);
    let mut cx = Context::from_waker(Waker::noop());
    let mut released = Box::pin(observer.released());
    assert!(
        Pin::as_mut(&mut released).poll(&mut cx).is_pending(),
        "source release waits while dependent cleanup is blocked"
    );
    assert_eq!(
        a_cleanup_calls.load(Ordering::SeqCst),
        0,
        "A cannot clean before B release proof"
    );
    assert!(
        source_scope.contains_resource(source_id),
        "accepted transfer leaves a source tombstone until receiver release proof"
    );

    let wrong_registry = qubit_operation::resource::ResourceRegistry::new(OperationId::new(9001), 1);
    let wrong_source_registry = qubit_operation::resource::ResourceRegistry::new(OperationId::new(9002), 1);
    let wrong_handle = wrong_source_registry.register(Box::new(ImmediateCleanup)).unwrap();
    let wrong_source = wrong_handle.id();
    wrong_handle.close_for_use().unwrap();
    let wrong_ready = wrong_source_registry.take_ready_cleanup().unwrap();
    let (wrong_source_id, wrong_owner) = wrong_ready.into_parts();
    let (_, accepted) = wrong_registry
        .accept_cleanup_transfer(wrong_source_id, wrong_owner)
        .unwrap();
    wrong_source_registry
        .confirm_transferred(wrong_source_id, accepted)
        .unwrap();
    let wrong_ready = wrong_registry.take_ready_cleanup().unwrap();
    let (wrong_receiver_id, wrong_owner) = wrong_ready.into_parts();
    let _ = block_on(wrong_owner.begin(CleanupContext { deadline: None }));
    let wrong_release = wrong_registry
        .confirm_released_with_receipt(wrong_receiver_id)
        .unwrap()
        .unwrap();
    assert_eq!(wrong_release.source(), wrong_source);
    assert_eq!(
        source_scope.confirm_transferred_release(source_id, wrong_release),
        Err(qubit_operation::resource::RegistryError::InvalidReceipt)
    );
    assert_eq!(a_cleanup_calls.load(Ordering::SeqCst), 0);
    drop(released);

    let ready = receiver.take_ready_cleanup().unwrap();
    let (receiver_id, owner) = ready.into_parts();
    assert!(matches!(
        block_on(owner.begin(CleanupContext { deadline: None })),
        qubit_operation::report::CleanupOutcome::Cleaned
    ));
    let release = receiver.confirm_released_with_receipt(receiver_id).unwrap().unwrap();
    source_scope.confirm_transferred_release(source_id, release).unwrap();
    let report = observer.released_blocking().unwrap();
    assert_eq!(
        a_cleanup_calls.load(Ordering::SeqCst),
        1,
        "B release proof unblocks and confirms A cleanup"
    );
    assert!(report.all_released);
    assert_eq!(report.transfers.len(), 1);
    assert!(!source_scope.contains_resource(source_id));
    assert_eq!(
        report,
        observer.released_blocking().unwrap(),
        "receiver proof does not rewrite the transfer history"
    );
}

#[test]
fn test_active_body_deadline_requests_timeout_and_publishes_stopped() {
    let clock = Arc::new(ManualClock::new(ClockDomainId(143)));
    let deadline = Deadline::at(MonotonicTime::new(ClockDomainId(143), 1));
    let gate = Gate::new();
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let supervisor = supervisor();
    let mut handle = supervisor
        .start(
            ScopeOptions {
                context: ContextOptions {
                    clock: Some(clock.clone()),
                    deadline: Some(deadline),
                    ..ContextOptions::default()
                },
                ..ScopeOptions::default()
            },
            {
                let gate = gate.clone();
                move |_| async move {
                    started_tx.send(()).unwrap();
                    gate.wait().await;
                    Ok::<_, std::io::Error>(8usize)
                }
            },
        )
        .unwrap();
    let started = started_rx.recv_timeout(std::time::Duration::from_secs(5));
    gate.wait_started();
    let _ = clock.advance_by(1);
    gate.release();
    let outcome = handle.result_blocking().unwrap();
    assert!(started.is_ok(), "body should start before deadline: {outcome:?}");
    assert!(
        matches!(outcome, OperationOutcome::Stopped(ref stop) if stop.reason == StopReason::Timeout),
        "deadline should stop active body, got {outcome:?}"
    );
}

#[test]
fn test_child_scope_deadline_stops_its_active_children() {
    let supervisor = supervisor();
    let clock = Arc::new(ManualClock::new(ClockDomainId(144)));
    let deadline = Deadline::at(MonotonicTime::new(ClockDomainId(144), 1));
    let control = ProbeControl::new(ChildBusinessOutcome::Succeeded, false);
    let body_control = control.clone();
    let body_clock = Arc::clone(&clock);
    let gate = Gate::new();
    let body_gate = gate.clone();
    let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(1);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let child = scope
                .child_scope(ScopeOptions {
                    context: ContextOptions {
                        clock: Some(body_clock),
                        deadline: Some(deadline),
                        ..ContextOptions::default()
                    },
                    ..ScopeOptions::default()
                })
                .unwrap();
            let mut reservation = child.reserve_child(ChildRequirement::Required).unwrap();
            reservation.attach(Arc::new(body_control.clone())).unwrap();
            reservation.commit().unwrap();
            entered_tx.send(()).unwrap();
            body_gate.wait().await;
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    entered_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    gate.wait_started();
    let _ = clock.advance_by(1);
    let stopped = control.stopped_before_watchdog();
    control.release();
    gate.release();
    assert!(stopped, "child deadline must stop its already committed child");
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert!(handle.released_blocking().unwrap().all_released);
}

#[test]
fn test_pending_child_reservations_consume_quota_and_uncommitted_drop_returns_it() {
    let supervisor = supervisor();
    let options = ScopeOptions {
        limits: ScopeLimits {
            child_operations: 1,
            ..ScopeLimits::default()
        },
        ..ScopeOptions::default()
    };
    let mut handle = supervisor
        .start(options, move |scope| async move {
            let first = scope.reserve_child(ChildRequirement::Required).unwrap();
            let second_is_limited = matches!(
                scope.reserve_child(ChildRequirement::Optional),
                Err(qubit_operation::scope::ScopeError::LimitReached)
            );
            drop(first);
            let returned = scope.reserve_child(ChildRequirement::Optional);
            let dropped_reservation_returns_quota = returned.is_ok();
            drop(returned);
            Ok::<_, std::io::Error>((second_is_limited, dropped_reservation_returns_quota))
        })
        .unwrap();
    let outcome = handle.result_blocking().unwrap();
    match outcome {
        OperationOutcome::Succeeded((second_is_limited, dropped_reservation_returns_quota)) => {
            assert!(second_is_limited, "pending reservations must consume child quota");
            assert!(
                dropped_reservation_returns_quota,
                "dropping an uncommitted reservation returns quota"
            );
        }
        other => panic!("unexpected operation outcome: {other:?}"),
    }
}

#[test]
fn test_async_body_error_under_fail_fast_stops_committed_child() {
    let control = ProbeControl::new(ChildBusinessOutcome::Succeeded, false);
    let control_for_body = control.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let supervisor = supervisor();
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let mut reservation = scope.reserve_child(ChildRequirement::Required).unwrap();
            reservation.attach(Arc::new(control_for_body.clone())).unwrap();
            reservation.commit().unwrap();
            tx.send(()).unwrap();
            Err::<(), _>("async body failed")
        })
        .unwrap();
    rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    let stopped = control.stopped_before_watchdog();
    control.release();
    let outcome = handle.result_blocking().unwrap();
    assert!(stopped, "FailFast must stop active committed children on body error");
    assert!(matches!(outcome, OperationOutcome::Failed("async body failed")));
}

#[test]
fn test_blocking_body_error_under_fail_fast_stops_committed_child() {
    let control = ProbeControl::new(ChildBusinessOutcome::Succeeded, false);
    let control_for_body = control.clone();
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let supervisor = supervisor();
    let executor = ThreadExecutor::new(1, 1).unwrap();
    let mut handle = supervisor
        .start_blocking(&executor, ScopeOptions::default(), move |scope| {
            let mut reservation = scope.reserve_child(ChildRequirement::Required).unwrap();
            reservation.attach(Arc::new(control_for_body.clone())).unwrap();
            reservation.commit().unwrap();
            tx.send(()).unwrap();
            Err::<(), _>("blocking body failed")
        })
        .unwrap();
    rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    let stopped = control.stopped_before_watchdog();
    control.release();
    let outcome = handle.result_blocking().unwrap();
    assert!(stopped, "FailFast must stop active committed children on body error");
    assert!(matches!(outcome, OperationOutcome::Failed("blocking body failed")));
}

#[test]
fn test_async_body_panic_publishes_panicked_and_settles_root() {
    let supervisor = supervisor();
    let (panic_tx, panic_rx) = std::sync::mpsc::channel();
    let mut handle = supervisor
        .start::<_, _, (), ()>(ScopeOptions::default(), move |_| async move {
            panic_tx.send(()).unwrap();
            panic!("contract body panic");
        })
        .unwrap();
    panic_rx
        .recv_timeout(std::time::Duration::from_secs(2))
        .expect("body entered before watchdog");
    let report = supervisor.shutdown_blocking(
        qubit_operation::supervisor::ShutdownMode::Drain,
        std::time::Duration::from_millis(20),
    );
    assert_eq!(report.pending_requests, 0);
    let mut result = Box::pin(handle.result());
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(matches!(
        result.as_mut().poll(&mut cx),
        Poll::Ready(Ok(OperationOutcome::Panicked(_)))
    ));
}

#[test]
fn test_cleanup_panic_publishes_unconfirmed_settlement_instead_of_stalling() {
    let supervisor = supervisor();
    let cleanup_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let body_cleanup_started = Arc::clone(&cleanup_started);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            scope.register_resource(PanicCleanup(body_cleanup_started)).unwrap();
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    let observer = handle.observer();
    let mut released = Box::pin(observer.released());
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    let report = loop {
        if let Poll::Ready(report) = released.as_mut().poll(&mut cx) {
            break Some(report);
        }
        if std::time::Instant::now() >= deadline {
            break None;
        }
        thread::yield_now();
    };
    assert!(cleanup_started.load(Ordering::SeqCst));
    assert!(report.is_some(), "cleanup panic must publish a terminal report");
    assert_eq!(report.unwrap().resource_phase, ResourcePhase::Unconfirmed);
}

#[test]
fn test_committed_effect_record_survives_later_cancellation() {
    let supervisor = supervisor();
    let gate = Gate::new();
    let body_gate = gate.clone();
    let (recorded_tx, recorded_rx) = std::sync::mpsc::sync_channel(1);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            scope
                .record_effect(EffectRecord {
                    operation_id: scope.operation_id(),
                    resource_id: None,
                    description: DiagnosticSummary::new("fake upload committed"),
                    fact: EffectFact::Known,
                })
                .unwrap();
            recorded_tx.send(()).unwrap();
            body_gate.wait().await;
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    recorded_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    gate.wait_started();
    handle.request_cancel(StopReason::UserCancelled);
    gate.release();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Stopped(ref stop) if stop.reason == StopReason::UserCancelled
    ));
    let report = handle.released_blocking().unwrap();
    assert_eq!(report.effects.len(), 1);
    assert_eq!(report.effects[0].operation_id, report.operation_id);
    assert_eq!(report.effects[0].fact, EffectFact::Known);
    assert_eq!(
        report.effects[0].description,
        DiagnosticSummary::new("fake upload committed")
    );
    assert_eq!(report.effect_overflow, 0);
}

#[test]
fn test_dropping_last_resource_use_wakes_scope_cleanup_drain() {
    let supervisor = supervisor();
    let cleanup_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let body_cleanup_calls = Arc::clone(&cleanup_calls);
    let (tx, rx) = std::sync::mpsc::sync_channel(1);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let id = scope.register_resource(CountCleanup(body_cleanup_calls)).unwrap();
            let resource_use = scope.acquire_use(id).unwrap();
            tx.send(resource_use).unwrap();
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    let resource_use = rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert!(matches!(
        handle.result_blocking().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    let mut released = Box::pin(handle.released());
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(released.as_mut().poll(&mut cx).is_pending());
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);
    drop(resource_use);
    assert!(handle.released_blocking().unwrap().all_released);
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn test_cleanup_pool_saturation_keeps_owner_until_capacity_returns() {
    let supervisor = OperationSupervisor::new(SupervisorLimits {
        cleanup_concurrency: 1,
        ..SupervisorLimits::default()
    });
    let first_gate = Gate::new();
    let first_cleanup_gate = first_gate.clone();
    let first_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let first_cleanup_started = Arc::clone(&first_started);
    let mut first = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            scope
                .register_resource(GateCleanup {
                    gate: first_cleanup_gate,
                    started: first_cleanup_started,
                })
                .unwrap();
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    assert!(matches!(
        first.result_blocking().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    first_gate.wait_started();

    let second_cleanup_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let body_cleanup_calls = Arc::clone(&second_cleanup_calls);
    let second_clock = Arc::new(ManualClock::new(ClockDomainId(211)));
    let mut second = supervisor
        .start(
            ScopeOptions {
                context: ContextOptions {
                    clock: Some(second_clock.clone()),
                    ..ContextOptions::default()
                },
                ..ScopeOptions::default()
            },
            move |scope| async move {
                scope.register_resource(CountCleanup(body_cleanup_calls)).unwrap();
                Ok::<_, std::io::Error>(())
            },
        )
        .unwrap();
    assert!(matches!(
        second.result_blocking().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    let observer = second.observer();
    first_gate.release();
    let clock = Arc::clone(&second_clock);
    let advance = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(20));
        let _ = clock.advance_by(1);
    });
    let observed = block_on(observer.observe_until(Deadline::at(MonotonicTime::new(ClockDomainId(211), 1)))).unwrap();
    advance.join().unwrap();
    assert!(observed.snapshot.settlement.is_some());
    assert_eq!(second_cleanup_calls.load(Ordering::SeqCst), 1);
    assert!(first.released_blocking().unwrap().all_released);
    assert!(second.released_blocking().unwrap().all_released);
}

#[test]
fn test_observer_deadline_returns_snapshot_and_keeps_observation_live() {
    let supervisor = supervisor();
    let gate = Gate::new();
    let clock = Arc::new(ManualClock::new(ClockDomainId(77)));
    let handle = supervisor
        .start(
            ScopeOptions {
                context: ContextOptions {
                    clock: Some(clock.clone()),
                    ..ContextOptions::default()
                },
                ..ScopeOptions::default()
            },
            {
                let gate = gate.clone();
                move |_| async move {
                    gate.wait().await;
                    Ok::<_, std::io::Error>(9usize)
                }
            },
        )
        .unwrap();
    gate.wait_started();
    let observer = handle.observer();
    let advancing_clock = Arc::clone(&clock);
    let deadline = Deadline::at(MonotonicTime::new(ClockDomainId(77), 1));
    let advancer = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(10));
        let _ = advancing_clock.advance_by(1);
    });
    let observed = block_on(observer.observe_until(deadline)).unwrap();
    advancer.join().unwrap();
    assert!(observed.snapshot.settlement.is_none());
    assert_eq!(observed.snapshot.resource_phase, ResourcePhase::Active);
    assert_eq!(
        clock.pending_timers(),
        0,
        "deadline registration is canceled after observation"
    );
    assert!(!gate.is_released());
    gate.release();
    gate.wait_released();
    let outcome = block_on(handle_result(handle));
    assert!(matches!(outcome, Ok(OperationOutcome::Succeeded(9))));
    assert!(block_on(observed.observer.released()).all_released);
}

#[test]
fn test_observer_deadline_without_context_clock_returns_explicit_error() {
    let supervisor = supervisor();
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |_| async move {
            Ok::<_, std::io::Error>(5usize)
        })
        .unwrap();
    let observer = handle.observer();
    let deadline = Deadline::at(MonotonicTime::new(ClockDomainId(78), 1));
    let error = match block_on(observer.observe_until(deadline)) {
        Ok(_) => panic!("observer deadline requires a configured clock"),
        Err(error) => error,
    };
    assert_eq!(error, qubit_operation::report::ObservationError::ClockUnavailable);
    assert!(matches!(
        block_on(handle.result()).unwrap(),
        OperationOutcome::Succeeded(5)
    ));
}

#[test]
fn test_start_wait_returns_unstarted_body_on_expired_context() {
    let supervisor = supervisor();
    let clock = Arc::new(ManualClock::new(ClockDomainId(88)));
    let deadline = Deadline::at(MonotonicTime::new(ClockDomainId(88), 1));
    let _ = clock.advance_by(1);
    let starts = Arc::new(AtomicU64::new(0));
    let body_starts = Arc::clone(&starts);
    let body = move |_| async move {
        body_starts.fetch_add(1, Ordering::SeqCst);
        Ok::<_, std::io::Error>(17usize)
    };
    let options = ScopeOptions {
        context: ContextOptions {
            clock: Some(clock),
            deadline: Some(deadline),
            ..ContextOptions::default()
        },
        ..ScopeOptions::default()
    };
    let error = match block_on(supervisor.start_wait(options, body)) {
        Ok(_) => panic!("expired context must reject admission"),
        Err(error) => error,
    };
    assert_eq!(error.reason(), qubit_operation::report::AdmissionError::DeadlineExpired);
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    let recovered = error.into_body();
    let mut handle = supervisor.start(ScopeOptions::default(), recovered).unwrap();
    assert!(matches!(
        block_on(handle.result()).unwrap(),
        OperationOutcome::Succeeded(17)
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
}

#[test]
fn test_start_wait_blocking_deadline_returns_original_body_while_capacity_is_held() {
    let limits = SupervisorLimits {
        active_roots: 1,
        queued_roots: 0,
        ..SupervisorLimits::default()
    };
    let supervisor = OperationSupervisor::new(limits);
    let gate = Gate::new();
    let active = supervisor
        .start(ScopeOptions::default(), {
            let gate = gate.clone();
            move |_| async move {
                gate.wait().await;
                Ok::<_, std::io::Error>(1usize)
            }
        })
        .unwrap();
    gate.wait_started();

    let clock = Arc::new(ManualClock::new(ClockDomainId(99)));
    let deadline = Deadline::at(MonotonicTime::new(ClockDomainId(99), 1));
    let advancing_clock = Arc::clone(&clock);
    let advancer = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(10));
        let _ = advancing_clock.advance_by(1);
    });
    let starts = Arc::new(AtomicU64::new(0));
    let body_starts = Arc::clone(&starts);
    let options = ScopeOptions {
        context: ContextOptions {
            clock: Some(clock),
            deadline: Some(deadline),
            ..ContextOptions::default()
        },
        ..ScopeOptions::default()
    };
    let error = match supervisor.start_wait_blocking(options, move |_| async move {
        body_starts.fetch_add(1, Ordering::SeqCst);
        Ok::<_, std::io::Error>(3usize)
    }) {
        Ok(_) => panic!("deadline must end the blocking admission wait"),
        Err(error) => error,
    };
    advancer.join().unwrap();
    assert_eq!(error.reason(), qubit_operation::report::AdmissionError::DeadlineExpired);
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    gate.release();
    let _ = block_on(active.released());
    let mut retry = supervisor.start(ScopeOptions::default(), error.into_body()).unwrap();
    assert!(matches!(
        block_on(retry.result()).unwrap(),
        OperationOutcome::Succeeded(3)
    ));
}

#[test]
fn test_start_waiter_is_admitted_after_active_root_releases() {
    let limits = SupervisorLimits {
        active_roots: 1,
        queued_roots: 0,
        ..SupervisorLimits::default()
    };
    let supervisor = OperationSupervisor::new(limits);
    let gate = Gate::new();
    let mut first = supervisor
        .start(ScopeOptions::default(), {
            let gate = gate.clone();
            move |_| async move {
                gate.wait().await;
                Ok::<_, std::io::Error>(1usize)
            }
        })
        .unwrap();
    gate.wait_started();
    let waiter_supervisor = supervisor.clone();
    let release_gate = gate.clone();
    let releaser = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(10));
        release_gate.release();
        let _ = block_on(first.result());
    });
    let mut second = block_on(
        waiter_supervisor.start_wait(ScopeOptions::default(), move |_| async move {
            Ok::<_, std::io::Error>(2usize)
        }),
    )
    .unwrap();
    releaser.join().unwrap();
    assert!(matches!(
        block_on(second.result()).unwrap(),
        OperationOutcome::Succeeded(2)
    ));
}

#[test]
fn test_dropping_pending_start_wait_removes_waiter_without_starting_body() {
    let limits = SupervisorLimits {
        active_roots: 1,
        queued_roots: 0,
        ..SupervisorLimits::default()
    };
    let supervisor = OperationSupervisor::new(limits);
    let gate = Gate::new();
    let active = supervisor
        .start(ScopeOptions::default(), {
            let gate = gate.clone();
            move |_| async move {
                gate.wait().await;
                Ok::<_, std::io::Error>(1usize)
            }
        })
        .unwrap();
    gate.wait_started();
    let starts = Arc::new(AtomicU64::new(0));
    let body_starts = Arc::clone(&starts);
    let mut waiter = Box::pin(supervisor.start_wait(ScopeOptions::default(), move |_| async move {
        body_starts.fetch_add(1, Ordering::SeqCst);
        Ok::<_, std::io::Error>(2usize)
    }));
    let waker = std::task::Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    assert!(waiter.as_mut().poll(&mut cx).is_pending());
    drop(waiter);
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    gate.release();
    let _ = block_on(active.released());
    assert_eq!(starts.load(Ordering::SeqCst), 0);
}

#[test]
fn test_start_wait_blocking_waits_for_capacity_then_submits() {
    let limits = SupervisorLimits {
        active_roots: 1,
        queued_roots: 0,
        ..SupervisorLimits::default()
    };
    let supervisor = OperationSupervisor::new(limits);
    let gate = Gate::new();
    let mut first = supervisor
        .start(ScopeOptions::default(), {
            let gate = gate.clone();
            move |_| async move {
                gate.wait().await;
                Ok::<_, std::io::Error>(1usize)
            }
        })
        .unwrap();
    gate.wait_started();
    let release_gate = gate.clone();
    let returned = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let returned_probe = Arc::clone(&returned);
    let releaser = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(10));
        assert!(
            !returned_probe.load(Ordering::SeqCst),
            "blocking admission must wait before capacity is released"
        );
        release_gate.release();
        let _ = block_on(first.result());
        let _ = block_on(first.released());
    });
    let mut second = supervisor
        .start_wait_blocking(ScopeOptions::default(), move |_| async move {
            Ok::<_, std::io::Error>(2usize)
        })
        .unwrap();
    returned.store(true, Ordering::SeqCst);
    releaser.join().unwrap();
    assert!(matches!(
        block_on(second.result()).unwrap(),
        OperationOutcome::Succeeded(2)
    ));
}

#[test]
fn test_start_wait_blocking_rejects_managed_worker() {
    let supervisor = supervisor();
    let nested_supervisor = supervisor.clone();
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |_| async move {
            let nested = nested_supervisor.start_wait_blocking(ScopeOptions::default(), move |_| async move {
                Ok::<_, std::io::Error>(())
            });
            let error = match nested {
                Ok(_) => panic!("managed worker must not block waiting for admission"),
                Err(error) => error,
            };
            assert_eq!(error.reason(), qubit_operation::report::AdmissionError::WouldDeadlock);
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    assert!(matches!(
        block_on(handle.result()).unwrap(),
        OperationOutcome::Succeeded(())
    ));
}

#[test]
fn test_start_blocking_rejects_managed_worker_without_running_body() {
    let supervisor = supervisor();
    let nested_supervisor = supervisor.clone();
    let executor = Arc::new(ThreadExecutor::new(1, 1).unwrap());
    let nested_executor = Arc::clone(&executor);
    let nested_started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let body_started = Arc::clone(&nested_started);
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |_| async move {
            let nested = nested_supervisor.start_blocking(&*nested_executor, ScopeOptions::default(), move |_| {
                body_started.store(true, Ordering::SeqCst);
                Ok::<_, std::io::Error>(())
            });
            let error = match nested {
                Ok(_) => panic!("managed worker must not submit blocking work synchronously"),
                Err(error) => error,
            };
            assert_eq!(error.reason(), qubit_operation::report::AdmissionError::WouldDeadlock);
            Ok::<_, std::io::Error>(())
        })
        .unwrap();
    assert!(matches!(
        block_on(handle.result()).unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert!(!nested_started.load(Ordering::SeqCst));
}

#[test]
fn test_start_blocking_runs_on_injected_executor_after_active_admission() {
    let supervisor = supervisor();
    let executor = ThreadExecutor::new(1, 1).unwrap();
    let worker_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_flag = Arc::clone(&worker_flag);
    let mut handle = supervisor
        .start_blocking(&executor, ScopeOptions::default(), move |_| {
            observed_flag.store(is_thread_executor_worker(), Ordering::SeqCst);
            Ok::<_, std::io::Error>(33usize)
        })
        .unwrap();
    assert!(matches!(
        block_on(handle.result()).unwrap(),
        OperationOutcome::Succeeded(33)
    ));
    assert!(worker_flag.load(Ordering::SeqCst));
}

#[test]
fn test_blocking_body_panic_publishes_panicked_and_releases_resources() {
    let supervisor = supervisor();
    let executor = ThreadExecutor::new(1, 1).unwrap();
    let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let body_started = Arc::clone(&started);
    let mut handle = supervisor
        .start_blocking(
            &executor,
            ScopeOptions::default(),
            move |_| -> Result<(), std::io::Error> {
                body_started.store(true, Ordering::SeqCst);
                panic!("fixture panic");
            },
        )
        .unwrap();
    let waker = std::task::Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut result = Box::pin(handle.result());
    assert!(matches!(result.as_mut().poll(&mut cx), Poll::Pending));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !started.load(Ordering::SeqCst) {
        assert!(
            std::time::Instant::now() < deadline,
            "blocking body begins before watchdog"
        );
        thread::yield_now();
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let outcome = loop {
        let outcome = result.as_mut().poll(&mut cx);
        if outcome.is_ready() {
            break outcome;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "panic result and release reach observer before watchdog"
        );
        thread::park_timeout(std::time::Duration::from_millis(10));
    };
    assert!(matches!(outcome, Poll::Ready(Ok(OperationOutcome::Panicked(_)))));
    drop(result);
    assert!(handle.released_blocking().unwrap().all_released);
}

#[test]
fn test_start_blocking_queue_rejection_returns_original_unstarted_body() {
    let supervisor = supervisor();
    let executor = ThreadExecutor::new(1, 1).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    assert!(
        executor
            .execute(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
            .is_ok()
    );
    started_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    assert!(executor.execute(|| {}).is_ok());
    let starts = Arc::new(AtomicU64::new(0));
    let body_starts = Arc::clone(&starts);
    let error = match supervisor.start_blocking(&executor, ScopeOptions::default(), move |_| {
        body_starts.fetch_add(1, Ordering::SeqCst);
        Ok::<_, std::io::Error>(44usize)
    }) {
        Ok(_) => panic!("full executor queue rejects before body start"),
        Err(error) => error,
    };
    assert_eq!(error.reason(), qubit_operation::report::AdmissionError::Busy);
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    let recovered = error.into_body();
    let mut handle = supervisor
        .start(ScopeOptions::default(), move |scope| async move { recovered(scope) })
        .unwrap();
    assert!(matches!(
        block_on(handle.result()).unwrap(),
        OperationOutcome::Succeeded(44)
    ));
    release_tx.send(()).unwrap();
}

#[test]
fn test_start_blocking_rolls_back_queued_root_and_original_body_can_retry() {
    let limits = SupervisorLimits {
        active_roots: 1,
        queued_roots: 1,
        ..SupervisorLimits::default()
    };
    let supervisor = OperationSupervisor::new(limits);
    let executor = ThreadExecutor::new(1, 1).unwrap();
    let gate = Gate::new();
    let first = supervisor
        .start(ScopeOptions::default(), {
            let gate = gate.clone();
            move |_| async move {
                gate.wait().await;
                Ok::<_, std::io::Error>(1usize)
            }
        })
        .unwrap();
    gate.wait_started();
    let second_gate = Gate::new();
    let mut second = supervisor
        .start(ScopeOptions::default(), {
            let second_gate = second_gate.clone();
            move |_| async move {
                second_gate.wait().await;
                Ok::<_, std::io::Error>(2usize)
            }
        })
        .unwrap();
    assert!(!second_gate.started());
    let release_first = gate.clone();
    let release_second = second_gate.clone();
    let releaser = thread::spawn(move || {
        thread::sleep(std::time::Duration::from_millis(10));
        release_first.release();
        let _ = block_on(first.released());
        release_second.wait_started();
        release_second.release();
        let _ = block_on(second.result());
    });
    let starts = Arc::new(AtomicU64::new(0));
    let body_starts = Arc::clone(&starts);
    let rejected = match supervisor.start_blocking(&executor, ScopeOptions::default(), move |_| {
        body_starts.fetch_add(1, Ordering::SeqCst);
        Ok::<_, std::io::Error>(3usize)
    }) {
        Ok(_) => panic!("synchronous start returns instead of waiting on a queued root"),
        Err(error) => error,
    };
    assert_eq!(rejected.reason(), qubit_operation::report::AdmissionError::Busy);
    assert_eq!(starts.load(Ordering::SeqCst), 0);
    let recovered = rejected.into_body();
    releaser.join().unwrap();
    let mut retry = supervisor
        .start_blocking(&executor, ScopeOptions::default(), recovered)
        .unwrap();
    assert!(matches!(
        block_on(retry.result()).unwrap(),
        OperationOutcome::Succeeded(3)
    ));
    assert_eq!(starts.load(Ordering::SeqCst), 1);
}

async fn handle_result<T, E>(
    mut handle: qubit_operation::operation::OperationHandle<T, E>,
) -> Result<OperationOutcome<T, E>, qubit_operation::report::ObservationError> {
    handle.result().await
}

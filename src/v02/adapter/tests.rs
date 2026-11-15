// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use super::AdapterErrorKind;
use super::AdapterReservation;
use super::ExternalTask;
use super::recover_lost_input;
use crate::v02::executor::CleanupExecutor;
use crate::v02::manager::capacity::ManagerCapacityPools;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::protocol::OperationControl;
use crate::v02::report::ChildOutcomeKind;
use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::ChildRelease;
use crate::v02::report::CleanupFailure;
use crate::v02::report::DiagnosticSummary;
use crate::v02::report::EffectRecord;
use crate::v02::report::ManagerId;
use crate::v02::report::ManagerLimits;
use crate::v02::report::OperationId;
use crate::v02::report::ResourceCleanupReport;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ScopeLimits;
use crate::v02::report::SettlementReport;
use crate::v02::report::SettlementStatus;
use crate::v02::report::StopReason;
use crate::v02::report::ThreadPoolExecutorOptions;
use crate::v02::report::TransferReceipt;
use crate::v02::resource::ResourceRef;
use crate::v02::resource::ResourceScope;
use crate::v02::resource::cleanup_port::CleanupDispatch;
use crate::v02::resource::cleanup_port::CleanupDispatchPermit;
use crate::v02::resource::cleanup_port::CleanupJob;
use crate::v02::resource::cleanup_port::CleanupQueueError;

#[derive(Default)]
struct Gate {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}

impl Gate {
    fn started(&self) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
    }

    fn wait_started(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.0 {
            state = self.changed.wait(state).unwrap();
        }
    }

    fn allow_return(&self) {
        let mut state = self.state.lock().unwrap();
        state.1 = true;
        self.changed.notify_all();
    }

    fn wait_for_return(&self) {
        let mut state = self.state.lock().unwrap();
        while !state.1 {
            state = self.changed.wait(state).unwrap();
        }
    }
}

struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct DeferredCleanup {
    jobs: Arc<Mutex<Vec<Box<dyn CleanupJob>>>>,
}

impl DeferredCleanup {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            jobs: Arc::new(Mutex::new(Vec::new())),
        })
    }

    fn schedule_all(&self) {
        let mut jobs = std::mem::take(&mut *self.jobs.lock().unwrap());
        if jobs.is_empty() {
            return;
        }
        let cleanup = Arc::new(
            CleanupExecutor::new(
                ThreadPoolExecutorOptions {
                    worker_count: 1,
                    queue_capacity: jobs.len().max(1),
                },
                Default::default(),
            )
            .expect("cleanup executor should initialize"),
        );
        for job in jobs.drain(..) {
            if let Err(returned) = job.schedule(Arc::clone(&cleanup)) {
                drop(returned);
            }
        }
        cleanup.shutdown();
    }
}

impl CleanupDispatch for DeferredCleanup {
    fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
        Ok(Box::new(DeferredPermit(Arc::clone(&self.jobs))))
    }
}

struct DeferredPermit(Arc<Mutex<Vec<Box<dyn CleanupJob>>>>);

impl CleanupDispatchPermit for DeferredPermit {
    fn enqueue(self: Box<Self>, job: Box<dyn CleanupJob>) {
        self.0.lock().unwrap().push(job);
    }
}

#[derive(Clone)]
struct DropSignal(Arc<(AtomicBool, Mutex<()>, Condvar)>);

impl DropSignal {
    fn new() -> Self {
        Self(Arc::new((AtomicBool::new(false), Mutex::new(()), Condvar::new())))
    }

    fn mark(&self) {
        let (dropped, _lock, changed) = &*self.0;
        dropped.store(true, Ordering::SeqCst);
        changed.notify_all();
    }

    fn wait_dropped(&self) {
        let (dropped, lock, changed) = &*self.0;
        let guard = lock.lock().unwrap();
        let (guard, timeout) = changed
            .wait_timeout_while(guard, Duration::from_secs(5), |_| !dropped.load(Ordering::SeqCst))
            .unwrap();
        assert!(!timeout.timed_out() || dropped.load(Ordering::SeqCst));
        drop(guard);
    }

    fn is_dropped(&self) -> bool {
        self.0.0.load(Ordering::SeqCst)
    }
}

struct PayloadDropFlag(Arc<AtomicBool>);

impl Drop for PayloadDropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct Tracked(DropSignal);

impl Drop for Tracked {
    fn drop(&mut self) {
        self.0.mark();
    }
}

fn managed_input() -> (ResourceScope, ResourceRef<Tracked>, Arc<DeferredCleanup>, DropSignal) {
    let manager = ManagerCapacityPools::new(ManagerLimits::default());
    let capacity = Arc::new(ScopeCapacity::root(&manager, 81, ScopeLimits::default()));
    let cleanup = DeferredCleanup::new();
    let scope = ResourceScope::new(
        ManagerId::new_unique(),
        ResourceScopeId::new_unique(),
        capacity,
        cleanup.clone(),
    );
    let signal = DropSignal::new();
    let input = scope
        .manage(Tracked(signal.clone()))
        .expect("resource should be managed");
    (scope, input, cleanup, signal)
}

fn outcome(id: OperationId) -> ChildOutcomeSummary {
    ChildOutcomeSummary {
        operation_id: id,
        kind: ChildOutcomeKind::Succeeded,
        summary: DiagnosticSummary::new("complete"),
    }
}

fn settlement(id: OperationId) -> SettlementReport {
    SettlementReport {
        operation_id: id,
        status: SettlementStatus::Released,
        released_count: 0,
        transferred_count: 0,
        recovery_count: 0,
        children: Arc::from([]),
        children_total: 0,
        children_truncated: false,
        cleanup_failures: Arc::<[CleanupFailure]>::from([]),
        cleanup_failures_total: 0,
        cleanup_failures_truncated: false,
        resource_reports: Arc::<[ResourceCleanupReport]>::from([]),
        resource_reports_total: 0,
        resource_reports_truncated: false,
        transfers: Arc::<[TransferReceipt]>::from([]),
        transfers_total: 0,
        transfers_truncated: false,
        recoveries: Arc::from([]),
        recoveries_total: 0,
        recoveries_truncated: false,
        effects: Arc::<[EffectRecord]>::from([]),
        effects_total: 0,
        effect_overflow: 0,
    }
}

fn context(waker: &Waker) -> Context<'_> {
    Context::from_waker(waker)
}

#[test]
fn test_stop_before_commit_abandons_once() {
    let id = OperationId::new_unique();
    let (scope, input, cleanup, dropped) = managed_input();
    let (payload, control) = ExternalTask::new(id, input);
    let abandon_task = payload.clone();
    let activated = Arc::new(AtomicUsize::new(0));
    let abandoned = Arc::new(AtomicUsize::new(0));
    let reservation = AdapterReservation::new(
        Box::new(()),
        {
            let activated = Arc::clone(&activated);
            move |_payload| {
                activated.fetch_add(1, Ordering::SeqCst);
            }
        },
        {
            let abandoned = Arc::clone(&abandoned);
            move || {
                abandoned.fetch_add(1, Ordering::SeqCst);
                abandon_task
                    .confirm_input_release(ChildRelease {
                        operation_id: id,
                        confirmed_at: None,
                    })
                    .expect("abandoning unstarted task releases its input");
            }
        },
    );
    let child = reservation
        .attach(control.clone(), payload)
        .expect("first attach should succeed");

    child.request_stop(StopReason::UserCancelled);
    let error = child.commit().expect_err("accepted stop must prevent commit");
    drop(error.payload);

    assert_eq!(activated.load(Ordering::SeqCst), 0);
    assert_eq!(abandoned.load(Ordering::SeqCst), 1);
    assert!(matches!(control.snapshot().stop, Some(stop) if stop.reason == StopReason::UserCancelled));
    assert!(!dropped.is_dropped(), "scope still owns the resource until it ends");
    scope.end();
    cleanup.schedule_all();
    dropped.wait_dropped();
}

#[test]
fn test_commit_rejection_drops_payload_before_abandon_notification() {
    let id = OperationId::new_unique();
    let (scope, input, cleanup, dropped) = managed_input();
    let (task, control) = ExternalTask::new(id, input);
    let payload_dropped = Arc::new(AtomicBool::new(false));
    let abandoned = Arc::new(AtomicUsize::new(0));
    let child = AdapterReservation::new_with_gates(
        Box::new(()),
        || Err(AdapterErrorKind::Stopped),
        || Ok(()),
        |_| panic!("rejected child must not activate"),
        {
            let payload_dropped = Arc::clone(&payload_dropped);
            let abandoned = Arc::clone(&abandoned);
            move || {
                assert!(payload_dropped.load(Ordering::SeqCst));
                abandoned.fetch_add(1, Ordering::SeqCst);
            }
        },
    )
    .attach(control, PayloadDropFlag(Arc::clone(&payload_dropped)))
    .expect("attach should succeed");

    let error = child.commit().expect_err("commit gate rejects start");
    drop(error.payload);

    assert_eq!(abandoned.load(Ordering::SeqCst), 1);
    task.confirm_input_release(ChildRelease {
        operation_id: id,
        confirmed_at: None,
    })
    .unwrap();
    scope.end();
    cleanup.schedule_all();
    dropped.wait_dropped();
}

#[test]
fn test_activation_rejection_drops_payload_before_abandon_notification() {
    let id = OperationId::new_unique();
    let (scope, input, cleanup, dropped) = managed_input();
    let (task, control) = ExternalTask::new(id, input);
    let payload_dropped = Arc::new(AtomicBool::new(false));
    let abandoned = Arc::new(AtomicUsize::new(0));
    let permit = AdapterReservation::new_with_gates(
        Box::new(()),
        || Ok(()),
        || Err(AdapterErrorKind::Stopped),
        |_| panic!("rejected child must not activate"),
        {
            let payload_dropped = Arc::clone(&payload_dropped);
            let abandoned = Arc::clone(&abandoned);
            move || {
                assert!(payload_dropped.load(Ordering::SeqCst));
                abandoned.fetch_add(1, Ordering::SeqCst);
            }
        },
    )
    .attach(control, PayloadDropFlag(Arc::clone(&payload_dropped)))
    .expect("attach should succeed")
    .commit()
    .expect("commit gate accepts start");

    let error = permit.activate().expect_err("activation gate rejects start");
    drop(error.payload);

    assert_eq!(abandoned.load(Ordering::SeqCst), 1);
    task.confirm_input_release(ChildRelease {
        operation_id: id,
        confirmed_at: None,
    })
    .unwrap();
    scope.end();
    cleanup.schedule_all();
    dropped.wait_dropped();
}

#[test]
fn test_dropping_reservation_releases_admission_before_abandon_notification() {
    let admission_dropped = Arc::new(AtomicBool::new(false));
    let abandoned = Arc::new(AtomicUsize::new(0));
    let reservation = AdapterReservation::new(
        Box::new(PayloadDropFlag(Arc::clone(&admission_dropped))),
        |_| panic!("unattached reservation must not activate"),
        {
            let admission_dropped = Arc::clone(&admission_dropped);
            let abandoned = Arc::clone(&abandoned);
            move || {
                assert!(admission_dropped.load(Ordering::SeqCst));
                abandoned.fetch_add(1, Ordering::SeqCst);
            }
        },
    );

    drop(reservation);

    assert_eq!(abandoned.load(Ordering::SeqCst), 1);
}

#[test]
fn test_dropping_attached_child_releases_payload_before_abandon_notification() {
    let id = OperationId::new_unique();
    let (scope, input, cleanup, dropped) = managed_input();
    let (task, control) = ExternalTask::new(id, input);
    let payload_dropped = Arc::new(AtomicBool::new(false));
    let abandoned = Arc::new(AtomicUsize::new(0));
    let child = AdapterReservation::new(Box::new(()), |_| panic!("dropped child must not activate"), {
        let payload_dropped = Arc::clone(&payload_dropped);
        let abandoned = Arc::clone(&abandoned);
        move || {
            assert!(payload_dropped.load(Ordering::SeqCst));
            abandoned.fetch_add(1, Ordering::SeqCst);
        }
    })
    .attach(control, PayloadDropFlag(Arc::clone(&payload_dropped)))
    .expect("attach should succeed");

    drop(child);

    assert_eq!(abandoned.load(Ordering::SeqCst), 1);
    task.confirm_input_release(ChildRelease {
        operation_id: id,
        confirmed_at: None,
    })
    .unwrap();
    scope.end();
    cleanup.schedule_all();
    dropped.wait_dropped();
}

#[test]
fn test_dropping_start_permit_releases_payload_before_abandon_notification() {
    let id = OperationId::new_unique();
    let (scope, input, cleanup, dropped) = managed_input();
    let (task, control) = ExternalTask::new(id, input);
    let payload_dropped = Arc::new(AtomicBool::new(false));
    let abandoned = Arc::new(AtomicUsize::new(0));
    let permit = AdapterReservation::new(Box::new(()), |_| panic!("dropped permit must not activate"), {
        let payload_dropped = Arc::clone(&payload_dropped);
        let abandoned = Arc::clone(&abandoned);
        move || {
            assert!(payload_dropped.load(Ordering::SeqCst));
            abandoned.fetch_add(1, Ordering::SeqCst);
        }
    })
    .attach(control, PayloadDropFlag(Arc::clone(&payload_dropped)))
    .expect("attach should succeed")
    .commit()
    .expect("commit should succeed");

    drop(permit);

    assert_eq!(abandoned.load(Ordering::SeqCst), 1);
    task.confirm_input_release(ChildRelease {
        operation_id: id,
        confirmed_at: None,
    })
    .unwrap();
    scope.end();
    cleanup.schedule_all();
    dropped.wait_dropped();
}

#[test]
fn test_child_activation_is_one_shot() {
    let id = OperationId::new_unique();
    let (_scope, input, cleanup, dropped) = managed_input();
    let (payload, control) = ExternalTask::new(id, input);
    let activated = Arc::new(AtomicUsize::new(0));
    let abandoned = Arc::new(AtomicUsize::new(0));
    let child = AdapterReservation::new(
        Box::new(()),
        {
            let activated = Arc::clone(&activated);
            move |_payload| {
                activated.fetch_add(1, Ordering::SeqCst);
            }
        },
        {
            let abandoned = Arc::clone(&abandoned);
            move || {
                abandoned.fetch_add(1, Ordering::SeqCst);
            }
        },
    )
    .attach(control, payload)
    .expect("attach should succeed");
    let permit = child.commit().expect("no stop was accepted");
    permit.activate().expect("start permit should activate once");

    assert_eq!(activated.load(Ordering::SeqCst), 1);
    assert_eq!(abandoned.load(Ordering::SeqCst), 0);
    drop(cleanup);
    drop(dropped);
}

#[test]
fn test_business_result_does_not_confirm_input_release() {
    let id = OperationId::new_unique();
    let (scope, input, cleanup, dropped) = managed_input();
    let (task, control) = ExternalTask::new(id, input);
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    assert!(task.poll_outcome(&mut context(&waker)).is_pending());

    task.publish_outcome(outcome(id));

    assert!(
        wakes.0.load(Ordering::SeqCst) > 0,
        "outcome publication wakes outcome observers"
    );
    assert!(matches!(
        task.poll_outcome(&mut context(&waker)),
        Poll::Ready(ChildOutcomeSummary {
            kind: ChildOutcomeKind::Succeeded,
            ..
        })
    ));
    assert!(task.poll_release(&mut context(&waker)).is_pending());
    assert!(!control.snapshot().input_released);
    assert!(!dropped.is_dropped());
    scope.end();
    assert!(!dropped.is_dropped(), "outcome cannot release the input owner");

    task.confirm_input_release(ChildRelease {
        operation_id: id,
        confirmed_at: None,
    })
    .expect("release confirmation should match the task");
    cleanup.schedule_all();
    dropped.wait_dropped();
}

#[test]
fn test_input_release_does_not_confirm_settlement() {
    let id = OperationId::new_unique();
    let (scope, input, cleanup, dropped) = managed_input();
    let (task, control) = ExternalTask::new(id, input);
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    assert!(control.poll_settlement(&mut context(&waker)).is_pending());

    task.confirm_input_release(ChildRelease {
        operation_id: id,
        confirmed_at: None,
    })
    .expect("release confirmation should match the task");
    scope.end();

    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        0,
        "releasing input must not wake settlement observers"
    );
    assert!(
        matches!(control.poll_release(&mut context(&waker)), Poll::Ready(ChildRelease { operation_id, .. }) if operation_id == id)
    );
    assert!(control.poll_settlement(&mut context(&waker)).is_pending());
    cleanup.schedule_all();
    dropped.wait_dropped();

    task.publish_settlement(settlement(id))
        .expect("first settlement should publish");
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(control.poll_settlement(&mut context(&waker)).is_ready());
}

#[test]
fn test_runtime_loss_retains_unconfirmed_input_owner() {
    let id = OperationId::new_unique();
    let (scope, input, cleanup, dropped) = managed_input();
    let (task, control) = ExternalTask::new(id, input);
    scope.end();

    task.mark_runtime_lost();

    assert!(task.is_unconfirmed());
    assert!(control.poll_release(&mut context(Waker::noop())).is_pending());
    assert!(control.poll_settlement(&mut context(Waker::noop())).is_pending());
    assert!(!control.snapshot().input_released);
    assert!(!dropped.is_dropped(), "lost runtime still owns the input reference");
    drop(task);
    drop(control);
    assert!(recover_lost_input(id), "lost reference must survive task handles");
    cleanup.schedule_all();
    dropped.wait_dropped();
}

#[test]
fn test_stopped_noninterruptible_work_retains_input_until_return() {
    let id = OperationId::new_unique();
    let (scope, input, cleanup, dropped) = managed_input();
    let (task, control) = ExternalTask::new(id, input);
    let gate = Arc::new(Gate::default());
    let worker_gate = Arc::clone(&gate);
    let worker = std::thread::spawn(move || {
        worker_gate.started();
        worker_gate.wait_for_return();
        task.confirm_input_release(ChildRelease {
            operation_id: id,
            confirmed_at: None,
        })
        .expect("worker return should release its input");
    });
    gate.wait_started();
    control.request_stop(StopReason::UserCancelled);
    scope.end();
    assert!(control.poll_release(&mut context(Waker::noop())).is_pending());
    assert!(!dropped.is_dropped());

    gate.allow_return();
    worker.join().expect("noninterruptible worker should return");
    cleanup.schedule_all();
    dropped.wait_dropped();
}

#[test]
fn test_stop_after_commit_prevents_child_activation() {
    let id = OperationId::new_unique();
    let (scope, input, cleanup, dropped) = managed_input();
    let (task, control) = ExternalTask::new(id, input);
    let activated = Arc::new(AtomicUsize::new(0));
    let abandoned = Arc::new(AtomicUsize::new(0));
    let permit = AdapterReservation::new(
        Box::new(()),
        {
            let activated = Arc::clone(&activated);
            move |_| {
                activated.fetch_add(1, Ordering::SeqCst);
            }
        },
        {
            let abandoned = Arc::clone(&abandoned);
            move || {
                abandoned.fetch_add(1, Ordering::SeqCst);
            }
        },
    )
    .attach(control.clone(), task.clone())
    .expect("attach should succeed")
    .commit()
    .expect("no stop was accepted before commit");

    control.request_stop(StopReason::UserCancelled);
    let error = permit.activate().expect_err("stop before activation must reject start");
    assert_eq!(error.reason, AdapterErrorKind::Stopped);
    drop(error.payload);

    assert_eq!(activated.load(Ordering::SeqCst), 0);
    assert_eq!(abandoned.load(Ordering::SeqCst), 1);
    assert!(matches!(control.snapshot().stop, Some(stop) if stop.reason == StopReason::UserCancelled));
    task.confirm_input_release(ChildRelease {
        operation_id: id,
        confirmed_at: None,
    })
    .expect("rejected child still confirms input release");
    scope.end();
    cleanup.schedule_all();
    dropped.wait_dropped();
}

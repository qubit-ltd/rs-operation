// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::Any;
use std::future::Future;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use qubit_operation::AsyncCleanup;
use qubit_operation::AsyncOperationManager;
use qubit_operation::CleanupContext;
use qubit_operation::CleanupFuture;
use qubit_operation::CleanupOutcome;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ResourceManageErrorKind;
use qubit_operation::ResourceProgress;
use qubit_operation::ShutdownMode;
use qubit_operation::ShutdownWait;
use qubit_operation::StopReason;
use qubit_operation::StopRequestStatus;

struct MarkerCleanup(Arc<std::sync::atomic::AtomicBool>);
impl AsyncCleanup<String> for MarkerCleanup {
    fn cleanup(self: Arc<Self>, resource: String, _context: CleanupContext) -> CleanupFuture<String> {
        Box::pin(async move {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            drop(resource);
            CleanupOutcome::Released
        })
    }
}

struct CountedDrop(Arc<std::sync::atomic::AtomicUsize>);

impl Drop for CountedDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

struct ProbeFuture {
    drops: Arc<std::sync::atomic::AtomicUsize>,
    panic_poll: bool,
    panic_drop: bool,
    return_error: bool,
    panic_error_drop: bool,
    _pin: PhantomPinned,
}

struct DropBomb;

impl Drop for DropBomb {
    fn drop(&mut self) {
        panic!("error value drop failed");
    }
}

impl Future for ProbeFuture {
    type Output = Result<(), Box<dyn Any + Send>>;

    fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_ref().get_ref();
        if this.panic_poll {
            panic!("future poll failed");
        }
        Poll::Ready(if this.return_error {
            Err(if this.panic_error_drop {
                Box::new(DropBomb)
            } else {
                Box::new("ready error")
            })
        } else {
            Ok(())
        })
    }
}

impl Drop for ProbeFuture {
    fn drop(&mut self) {
        self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.panic_drop {
            panic!("future drop failed");
        }
    }
}

#[test]
fn test_real_sync_and_async_managers_run_typed_operations_and_shutdown() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut sync = manager
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(17usize))
        .unwrap();
    assert!(matches!(sync.wait_outcome().unwrap(), OperationOutcome::Succeeded(17)));
    assert!(sync.wait_settlement().unwrap().all_released());
    assert!(matches!(
        manager
            .request_shutdown(ShutdownMode::Drain)
            .wait_for(Duration::from_secs(3))
            .unwrap(),
        ShutdownWait::Completed(_)
    ));

    let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
    let mut asynchronous = manager
        .try_submit(OperationOptions::default(), |_| async { Ok::<_, ()>(23usize) })
        .unwrap()
        .into_sync();
    assert!(matches!(
        asynchronous.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(23)
    ));
    assert!(asynchronous.wait_settlement().unwrap().all_released());
    assert!(matches!(
        manager
            .as_sync()
            .request_shutdown(ShutdownMode::Drain)
            .wait_for(Duration::from_secs(3))
            .unwrap(),
        ShutdownWait::Completed(_)
    ));
}

#[test]
fn test_async_body_construction_panic_is_reported_and_releases_capacity() {
    let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
    let resource_drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let resource_drops_for_body = Arc::clone(&resource_drops);
    let mut operation = manager
        .try_submit(
            OperationOptions::default(),
            move |scope| -> std::future::Ready<Result<(), ()>> {
                let _resource = scope
                    .manage(CountedDrop(resource_drops_for_body))
                    .expect("resource should be accepted before body construction panics");
                panic!("future construction failed")
            },
        )
        .unwrap()
        .into_sync();

    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        sender
            .send((operation.wait_outcome(), operation.wait_settlement()))
            .unwrap();
    });
    let (outcome, settlement) = receiver
        .recv_timeout(Duration::from_secs(3))
        .expect("accepted operation should finish after a construction panic");
    assert!(matches!(outcome.unwrap(), OperationOutcome::Panicked(_)));
    assert!(settlement.unwrap().all_released());
    assert_eq!(resource_drops.load(std::sync::atomic::Ordering::SeqCst), 1);

    let mut next = manager
        .try_submit(OperationOptions::default(), |_| async { Ok::<_, ()>(()) })
        .unwrap()
        .into_sync();
    assert!(matches!(next.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
    assert!(next.wait_settlement().unwrap().all_released());
}

#[test]
fn test_async_body_poll_panic_is_reported_and_releases_capacity() {
    let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
    let mut operation = manager
        .try_submit(OperationOptions::default(), |_| async {
            panic!("poll failed");
            #[allow(unreachable_code)]
            Ok::<(), ()>(())
        })
        .unwrap()
        .into_sync();

    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        sender
            .send((operation.wait_outcome(), operation.wait_settlement()))
            .unwrap();
    });
    let (outcome, settlement) = receiver
        .recv_timeout(Duration::from_secs(3))
        .expect("accepted operation should finish after a poll panic");
    assert!(matches!(outcome.unwrap(), OperationOutcome::Panicked(_)));
    assert!(settlement.unwrap().all_released());

    let mut next = manager
        .try_submit(OperationOptions::default(), |_| async { Ok::<_, ()>(()) })
        .unwrap()
        .into_sync();
    assert!(matches!(next.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
    assert!(next.wait_settlement().unwrap().all_released());
}

#[test]
fn test_async_body_future_drop_panic_is_reported_and_releases_capacity() {
    let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let drops_for_body = Arc::clone(&drops);
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |_| ProbeFuture {
            drops: drops_for_body,
            panic_poll: false,
            panic_drop: true,
            return_error: false,
            panic_error_drop: false,
            _pin: PhantomPinned,
        })
        .unwrap()
        .into_sync();

    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        sender
            .send((operation.wait_outcome(), operation.wait_settlement()))
            .unwrap();
    });
    let (outcome, settlement) = receiver
        .recv_timeout(Duration::from_secs(3))
        .expect("accepted operation should finish after a future drop panic");
    assert!(matches!(outcome.unwrap(), OperationOutcome::Panicked(_)));
    assert!(settlement.unwrap().all_released());
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);

    let mut next = manager
        .try_submit(OperationOptions::default(), |_| async { Ok::<_, ()>(()) })
        .unwrap()
        .into_sync();
    assert!(matches!(next.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
    assert!(next.wait_settlement().unwrap().all_released());
}

#[test]
fn test_async_body_poll_and_drop_panics_preserve_poll_panic() {
    let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let drops_for_body = Arc::clone(&drops);
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |_| ProbeFuture {
            drops: drops_for_body,
            panic_poll: true,
            panic_drop: true,
            return_error: false,
            panic_error_drop: false,
            _pin: PhantomPinned,
        })
        .unwrap()
        .into_sync();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        sender
            .send((operation.wait_outcome(), operation.wait_settlement()))
            .unwrap()
    });
    let (outcome, settlement) = receiver
        .recv_timeout(Duration::from_secs(3))
        .expect("poll and final drop panics should be contained");
    let OperationOutcome::Panicked(panic) = outcome.unwrap() else {
        panic!("poll panic should be reported")
    };
    assert!(panic.summary.as_str().contains("future poll failed"));
    assert!(settlement.unwrap().all_released());
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn test_async_body_ready_error_drops_future_once() {
    let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let drops_for_body = Arc::clone(&drops);
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |_| ProbeFuture {
            drops: drops_for_body,
            panic_poll: false,
            panic_drop: false,
            return_error: true,
            panic_error_drop: false,
            _pin: PhantomPinned,
        })
        .unwrap()
        .into_sync();
    assert!(matches!(operation.wait_outcome().unwrap(), OperationOutcome::Failed(_)));
    assert!(operation.wait_settlement().unwrap().all_released());
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn test_async_body_drop_panic_safely_discards_error_with_panicking_drop() {
    let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let drops_for_body = Arc::clone(&drops);
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |_| ProbeFuture {
            drops: drops_for_body,
            panic_poll: false,
            panic_drop: true,
            return_error: true,
            panic_error_drop: true,
            _pin: PhantomPinned,
        })
        .unwrap()
        .into_sync();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        sender
            .send((operation.wait_outcome(), operation.wait_settlement()))
            .unwrap()
    });
    let (outcome, settlement) = receiver
        .recv_timeout(Duration::from_secs(3))
        .expect("discarding an error with a panicking destructor should stay contained");
    assert!(matches!(outcome.unwrap(), OperationOutcome::Panicked(_)));
    assert!(settlement.unwrap().all_released());
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[test]
fn test_operation_scope_resource_scope_uses_requested_local_capacity() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let completion = manager
        .run_and_settle(OperationOptions::default(), |scope| {
            let resources = scope
                .resource_scope(qubit_operation::ResourceScopeOptions { capacity: 1 })
                .unwrap();
            let first = resources.manage(11usize).unwrap();
            let rejected = resources.manage(12usize);
            assert!(rejected.is_err());
            assert_eq!(*first, 11);
            drop(first);
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(completion.outcome, OperationOutcome::Succeeded(())));
    assert!(completion.settlement.all_released());
}

#[test]
fn test_operation_scope_reservation_tracks_progress_and_can_be_retained_after_end() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (sender, receiver) = mpsc::channel();
    let gate = Arc::new(Barrier::new(2));
    let body_gate = Arc::clone(&gate);
    let cleanup_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleanup_for_task = Arc::clone(&cleanup_ran);
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let reservation = scope.reserve_resource().unwrap();
            sender.send(reservation).unwrap();
            body_gate.wait();
            Ok::<_, ()>(())
        })
        .unwrap();

    let reservation = receiver.recv().unwrap();
    assert_eq!(operation.snapshot().resources, Some(ResourceProgress::ScopeOpen));
    gate.wait();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert_eq!(operation.snapshot().resources, Some(ResourceProgress::WaitingForUsers));

    let error = match reservation.commit_with(String::from("retained input"), MarkerCleanup(cleanup_for_task)) {
        Ok(_) => panic!("ended operation scope accepted a resource commit"),
        Err(error) => error,
    };
    let (reason, resource, cleanup, reservation) = error.into_parts();
    assert_eq!(reason, ResourceManageErrorKind::ScopeEnded);
    assert_eq!(resource, "retained input");
    let resource_id = reservation.retain_with_for_cleanup(resource, cleanup);
    let settlement = operation.wait_settlement().unwrap();
    assert!(settlement.all_released());
    assert!(cleanup_ran.load(std::sync::atomic::Ordering::SeqCst));
    assert!(
        settlement
            .resource_reports
            .iter()
            .any(|report| { report.resource_id == resource_id })
    );
    assert_eq!(operation.snapshot().resources, None);
}

#[test]
fn test_accepted_stop_rejects_reserved_resource_commit_and_cleanup_settles_once() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (sender, receiver) = mpsc::channel();
    let (id_sender, id_receiver) = mpsc::channel();
    let gate = Arc::new(Barrier::new(2));
    let body_gate = Arc::clone(&gate);
    let cleanup_ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cleanup_for_task = Arc::clone(&cleanup_ran);
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let reservation = scope.reserve_resource().unwrap();
            sender.send(()).unwrap();
            body_gate.wait();
            let error = match reservation.commit_with(String::from("stopped owner"), MarkerCleanup(cleanup_for_task)) {
                Ok(resource) => {
                    drop(resource);
                    return Ok::<_, ()>(None);
                }
                Err(error) => error,
            };
            let (reason, resource, cleanup, reservation) = error.into_parts();
            assert_eq!(reason, ResourceManageErrorKind::Stopped);
            let resource_id = reservation.retain_with_for_cleanup(resource, cleanup);
            id_sender.send(resource_id).unwrap();
            Ok(Some(resource_id))
        })
        .unwrap();

    receiver.recv().unwrap();
    assert_eq!(
        operation.request_stop(StopReason::UserCancelled),
        StopRequestStatus::Accepted
    );
    gate.wait();
    let outcome = operation.wait_outcome().unwrap();
    assert!(matches!(outcome, OperationOutcome::Stopped(_)));
    let settlement = operation.wait_settlement().unwrap();
    assert!(settlement.all_released());
    assert_eq!(settlement.resource_reports.len(), 1);
    let resource_id = id_receiver.recv().unwrap();
    assert!(cleanup_ran.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(settlement.resource_reports[0].resource_id, resource_id);
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Barrier;
use std::sync::mpsc;
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

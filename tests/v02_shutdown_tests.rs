// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

use qubit_operation::AdmissionError;
use qubit_operation::AsyncCleanup;
use qubit_operation::CleanupContext;
use qubit_operation::CleanupFailure;
use qubit_operation::CleanupFuture;
use qubit_operation::CleanupOutcome;
use qubit_operation::CleanupStage;
use qubit_operation::DiagnosticSummary;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::RecoveryStore;
use qubit_operation::RecoveryStoreOptions;
use qubit_operation::ResourceId;
use qubit_operation::ResourceScopeOptions;
use qubit_operation::ShutdownMode;
use qubit_operation::ShutdownWait;

#[test]
fn test_drain_closes_roots_and_completes_when_idle() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let shutdown = manager.request_shutdown(ShutdownMode::Drain);
    let result = manager.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
    assert!(matches!(result, Err(error) if error.reason() == AdmissionError::Closed));
    let report = shutdown.wait().unwrap();
    assert_eq!(report.snapshot.active_operations, 0);
}

#[test]
fn test_drain_rejects_new_roots_but_keeps_open_descendants_admissible() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (bound_tx, bound_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            bound_tx.send(scope.manager()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let bound = bound_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let shutdown = manager.request_shutdown(ShutdownMode::Drain);
    assert!(matches!(
        manager.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(())),
        Err(error) if error.reason() == AdmissionError::Closed
    ));
    let mut child = bound
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
        .unwrap();
    assert!(matches!(child.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
    release_tx.send(()).unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    parent.wait_settlement().unwrap();
    assert!(matches!(
        shutdown.wait_for(Duration::from_secs(5)).unwrap(),
        ShutdownWait::Completed(_)
    ));
}

#[test]
fn test_cancel_waits_for_noninterruptible_body_to_return() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let shutdown = manager.request_shutdown(ShutdownMode::Cancel);
    assert!(matches!(
        shutdown.wait_for(Duration::ZERO).unwrap(),
        ShutdownWait::Pending(_)
    ));
    release_tx.send(()).unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Stopped(_)
    ));
    operation.wait_settlement().unwrap();
    assert!(matches!(
        shutdown.wait_for(Duration::from_secs(5)).unwrap(),
        ShutdownWait::Completed(_)
    ));
}

struct FailingCleanup(Arc<Mutex<Option<ResourceId>>>);

impl AsyncCleanup<usize> for FailingCleanup {
    fn cleanup(self: Arc<Self>, resource: usize, _context: CleanupContext) -> CleanupFuture<usize> {
        Box::pin(future::ready(CleanupOutcome::Failed {
            failure: CleanupFailure {
                resource_id: self.0.lock().unwrap().unwrap(),
                stage: CleanupStage::AsyncRelease,
                summary: DiagnosticSummary::new("retry required"),
                source: None,
            },
            resource,
        }))
    }
}

#[test]
fn test_recovery_blocks_shutdown_until_signed_transfer() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let completion = manager
        .run_and_settle(OperationOptions::default(), |scope| {
            let resources = scope.resource_scope(ResourceScopeOptions { capacity: 1 }).unwrap();
            let resource_id = Arc::new(Mutex::new(None));
            let resource = resources
                .manage_with(17usize, Arc::new(FailingCleanup(Arc::clone(&resource_id))))
                .unwrap();
            *resource_id.lock().unwrap() = Some(resource.id());
            drop(resource);
            Ok::<_, ()>(())
        })
        .unwrap();
    assert_eq!(completion.settlement.recovery_count, 1);
    let recovery = completion.settlement.recoveries[0].clone();
    let shutdown = manager.request_shutdown(ShutdownMode::Drain);
    assert_eq!(shutdown.snapshot().recovery_groups, 1);
    assert!(matches!(
        shutdown.wait_for(Duration::ZERO).unwrap(),
        ShutdownWait::Pending(snapshot) if snapshot.recovery_groups == 1
    ));

    let store = RecoveryStore::new(RecoveryStoreOptions::default()).unwrap();
    let transfer = recovery.transfer_to(&store).unwrap();
    assert_eq!(transfer.recovery, recovery.id());
    assert!(store.contains(recovery.id()));
    assert!(matches!(
        shutdown.wait_for(Duration::from_secs(2)).unwrap(),
        ShutdownWait::Completed(report) if report.snapshot.recovery_groups == 0
    ));
}

#[test]
fn test_shutdown_can_upgrade_not_reopen() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let drain = manager.request_shutdown(ShutdownMode::Drain);
    assert_eq!(drain.mode(), ShutdownMode::Drain);
    let cancel = manager.request_shutdown(ShutdownMode::Cancel);
    assert_eq!(cancel.mode(), ShutdownMode::Cancel);
    manager.close_admission();
    assert_eq!(
        manager.request_shutdown(ShutdownMode::Drain).mode(),
        ShutdownMode::Cancel
    );
    let rejected = manager.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
    assert!(matches!(
        rejected,
        Err(error) if error.reason() == AdmissionError::Closed
    ));
    assert!(matches!(
        cancel.wait_for(Duration::from_secs(2)).unwrap(),
        ShutdownWait::Completed(_)
    ));
}

#[test]
fn test_bound_manager_shutdown_is_global() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (bound_tx, bound_rx) = mpsc::channel();
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |scope| {
            bound_tx.send(scope.manager()).unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let bound = bound_rx.recv().unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    let shutdown = bound.request_shutdown(ShutdownMode::Drain);
    let rejected = manager.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
    assert!(matches!(
        rejected,
        Err(error) if error.reason() == AdmissionError::Closed
    ));
    assert!(matches!(
        shutdown.wait_for(Duration::from_secs(2)).unwrap(),
        ShutdownWait::Completed(_)
    ));
}

#[test]
fn test_drain_rejects_root_allows_open_descendants() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (bound_tx, bound_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            bound_tx.send(scope.manager()).unwrap();
            finish_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let shutdown = manager.request_shutdown(ShutdownMode::Drain);
    assert!(matches!(
        manager.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(())),
        Err(error) if error.reason() == AdmissionError::Closed
    ));
    let bound = bound_rx.recv().unwrap();
    let mut child = bound
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
        .unwrap();
    assert!(matches!(child.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
    finish_tx.send(()).unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    parent.wait_settlement().unwrap();
    assert!(matches!(
        shutdown.wait_for(Duration::from_secs(2)).unwrap(),
        ShutdownWait::Completed(_)
    ));
}

#[test]
fn test_cancel_blocks_commit_and_propagates() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (bound_tx, bound_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            bound_tx.send(scope.manager()).unwrap();
            finish_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let bound = bound_rx.recv().unwrap();
    let shutdown = manager.request_shutdown(ShutdownMode::Cancel);
    assert!(matches!(
        bound.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(())),
        Err(error) if error.reason() == AdmissionError::Stopped
    ));
    finish_tx.send(()).unwrap();
    assert!(matches!(parent.wait_outcome().unwrap(), OperationOutcome::Stopped(_)));
    parent.wait_settlement().unwrap();
    assert!(matches!(
        shutdown.wait_for(Duration::from_secs(2)).unwrap(),
        ShutdownWait::Completed(_)
    ));
}

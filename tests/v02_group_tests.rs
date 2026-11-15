// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::sync::mpsc;
use std::task::Context;

use qubit_operation::AdmissionError;
use qubit_operation::GroupOptions;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ShutdownMode;
use qubit_operation::ShutdownWait;

#[test]
fn test_group_limit_is_local_and_released_after_settlement() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let mut held = group
        .try_submit(OperationOptions::default(), move |_| {
            started_tx.send(()).unwrap();
            finish_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    started_rx.recv().unwrap();
    let rejected = group.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
    assert!(matches!(rejected, Err(error) if error.reason() == AdmissionError::Busy));

    let mut outside = manager
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
        .unwrap();
    assert!(matches!(
        outside.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    finish_tx.send(()).unwrap();
    assert!(matches!(
        held.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    held.wait_settlement().unwrap();
    assert_eq!(group.snapshot().active_operations, 0);
    assert!(
        group
            .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
            .is_ok()
    );
}

#[test]
fn test_async_group_submit_waits_without_immediate_retry_when_group_is_full() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut held = group
        .try_submit(OperationOptions::default(), move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    started_rx.recv().unwrap();

    let async_group = group.as_async();
    let mut waiting =
        Box::pin(async_group.submit(OperationOptions::default(), |_| async { Ok::<_, ()>(()) }));
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        waiting.as_mut().poll(&mut context),
        std::task::Poll::Pending
    ));

    release_tx.send(()).unwrap();
    assert!(matches!(
        held.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    held.wait_settlement().unwrap();
    assert!(matches!(
        waiting.as_mut().poll(&mut context),
        std::task::Poll::Ready(Ok(_))
    ));
}

#[test]
fn test_group_close_interrupts_a_capacity_waiter() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut held = group
        .try_submit(OperationOptions::default(), move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    started_rx.recv().unwrap();

    let async_group = group.as_async();
    let body_owner = std::sync::Arc::new(());
    let mut waiting = Box::pin(async_group.submit(OperationOptions::default(), move |_| {
        let _ = &body_owner;
        async { Ok::<_, ()>(()) }
    }));
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        waiting.as_mut().poll(&mut context),
        std::task::Poll::Pending
    ));

    group.close_admission();
    let error = match waiting.as_mut().poll(&mut context) {
        std::task::Poll::Ready(Err(error)) => error,
        _ => panic!("group close should interrupt a root capacity waiter"),
    };
    let (reason, body) = error.into_parts();
    assert_eq!(reason, AdmissionError::Closed);
    drop(body);

    release_tx.send(()).unwrap();
    let _ = held.wait_outcome().unwrap();
}

#[test]
fn test_sync_group_run_and_settle_uses_group_target() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let completion = group
        .run_and_settle(OperationOptions::default(), |_| Ok::<_, ()>(91usize))
        .unwrap();
    assert!(matches!(
        completion.outcome,
        OperationOutcome::Succeeded(91)
    ));
    assert!(completion.settlement.all_released());
}

#[test]
fn test_group_wait_for_max_waits_for_active_operation() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut operation = group
        .try_submit(OperationOptions::default(), move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    started_rx.recv().unwrap();
    let shutdown = group.request_shutdown(ShutdownMode::Drain);
    let release = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(20));
        release_tx.send(()).unwrap();
    });
    let outcome = std::thread::spawn(move || operation.wait_outcome().unwrap());
    assert!(matches!(
        shutdown.wait_for(std::time::Duration::MAX).unwrap(),
        ShutdownWait::Completed(_)
    ));
    release.join().unwrap();
    assert!(matches!(
        outcome.join().unwrap(),
        OperationOutcome::Succeeded(())
    ));
}

#[test]
fn test_local_drain_rejects_group_roots_but_accepts_bound_descendants() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let (bound_tx, bound_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let mut parent = group
        .try_submit(OperationOptions::default(), move |scope| {
            bound_tx.send(scope.manager()).unwrap();
            finish_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let bound = bound_rx.recv().unwrap();
    group.close_admission();
    assert!(
        matches!(group.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(())),
        Err(error) if error.reason() == AdmissionError::Closed)
    );
    let mut child = bound
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
        .unwrap();
    assert!(matches!(
        child.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    finish_tx.send(()).unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    parent.wait_settlement().unwrap();
}

#[test]
fn test_local_cancel_stops_group_without_closing_manager() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let (finish_tx, finish_rx) = mpsc::channel();
    let mut operation = group
        .try_submit(OperationOptions::default(), move |_| {
            finish_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let shutdown = group.request_shutdown(ShutdownMode::Cancel);
    assert!(
        matches!(group.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(())),
        Err(error) if error.reason() == AdmissionError::Stopped)
    );
    let mut outside = manager
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
        .unwrap();
    finish_tx.send(()).unwrap();
    assert!(matches!(
        outside.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Stopped(_)
    ));
    operation.wait_settlement().unwrap();
    let report = shutdown.wait().unwrap();
    assert_eq!(report.snapshot.active_operations, 0);
}

#[test]
fn test_async_group_keeps_group_target_and_local_shutdown() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let sync_group = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let async_group = sync_group.as_async();
    let mut operation = async_group
        .try_submit(OperationOptions::default(), |_| async {
            Ok::<_, ()>(73usize)
        })
        .unwrap()
        .into_sync();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(73)
    ));
    operation.wait_settlement().unwrap();
    let shutdown = async_group.request_shutdown(ShutdownMode::Drain);
    assert!(matches!(
        shutdown
            .as_sync()
            .wait_for(std::time::Duration::from_secs(2))
            .unwrap(),
        ShutdownWait::Completed(_)
    ));
    assert!(
        manager
            .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
            .is_ok()
    );
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::time::Duration;

use qubit_operation::AdmissionError;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::PortableExecutorOptions;
use qubit_operation::ScopeLimits;
use qubit_operation::ShutdownMode;

struct ThreadWaker(std::thread::Thread);

impl Wake for ThreadWaker {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = std::task::Waker::from(std::sync::Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}

#[test]
fn test_independent_new_managers_do_not_share_capacity_state() {
    let first = OperationManager::new(ManagerOptions::default()).unwrap();
    let second = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut a = first
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(1))
        .unwrap();
    let mut b = second
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(2))
        .unwrap();
    assert!(matches!(
        a.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(1)
    ));
    assert!(matches!(
        b.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(2)
    ));
    a.wait_settlement().unwrap();
    b.wait_settlement().unwrap();
    let first_report = first.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    let second_report = second.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(first_report.snapshot.active_operations, 0);
    assert_eq!(second_report.snapshot.active_operations, 0);
}

#[test]
fn test_async_executor_full_waits_for_executor_capacity() {
    let manager = OperationManager::new(ManagerOptions {
        async_executor: PortableExecutorOptions {
            task_capacity: 1,
            ..PortableExecutorOptions::default()
        },
        ..ManagerOptions::default()
    })
    .unwrap();
    let async_manager = manager.as_async();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut held = async_manager
        .try_submit(OperationOptions::default(), move |_| async move {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();

    let mut waiting =
        Box::pin(async_manager.submit(OperationOptions::default(), |_| async { Ok::<_, ()>(()) }));
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        waiting.as_mut().poll(&mut context),
        std::task::Poll::Pending
    ));

    release_tx.send(()).unwrap();
    assert!(matches!(
        block_on(held.wait_outcome()).unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert!(matches!(
        waiting.as_mut().poll(&mut context),
        std::task::Poll::Ready(Ok(_))
    ));
}

#[test]
fn test_a_parent_child_slot_stays_reserved_until_the_child_really_finishes() {
    let manager_options = ManagerOptions {
        scope_limits: ScopeLimits {
            child_operations: 1,
            ..ScopeLimits::default()
        },
        ..ManagerOptions::default()
    };
    let manager = OperationManager::new(manager_options).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (rejection_tx, rejection_rx) = mpsc::channel();
    let (child_handle_tx, child_handle_rx) = mpsc::channel();
    let (continue_tx, continue_rx) = mpsc::channel();
    let (followup_tx, followup_rx) = mpsc::channel();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let child = scope
                .manager()
                .try_submit(OperationOptions::default(), move |_| {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok::<_, ()>(())
                })
                .unwrap();
            child_handle_tx.send(child).unwrap();
            let rejected = scope
                .manager()
                .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
            rejection_tx.send(rejected.err().unwrap().reason()).unwrap();
            continue_rx.recv().unwrap();
            let followup = scope
                .manager()
                .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
                .map_err(|error| error.reason())
                .map(|mut handle| {
                    assert!(matches!(
                        handle.wait_outcome().unwrap(),
                        OperationOutcome::Succeeded(())
                    ));
                    handle.wait_settlement().unwrap();
                });
            followup_tx.send(followup.is_ok()).unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();

    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(
        rejection_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        AdmissionError::Busy
    );
    let mut child = child_handle_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let child_observer = child.observer();
    let before_release = child_observer.snapshot();
    assert!(
        before_release.stop.is_none(),
        "a Busy rejection must not stop the accepted child: {before_release:?}"
    );
    release_tx.send(()).unwrap();
    assert!(matches!(
        child.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    child.wait_settlement().unwrap();
    continue_tx.send(()).unwrap();
    assert!(followup_rx.recv_timeout(Duration::from_secs(5)).unwrap());
    let parent_outcome = parent.wait_outcome().unwrap();
    assert!(
        matches!(parent_outcome, OperationOutcome::Succeeded(())),
        "{parent_outcome:?}"
    );
    let after_release = child_observer.snapshot();
    assert!(
        after_release.stop.is_none(),
        "the accepted child was stopped unexpectedly: {after_release:?}"
    );
    assert!(parent.wait_settlement().unwrap().all_released());
    let shutdown_report = manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .unwrap();
    assert_eq!(shutdown_report.snapshot.active_operations, 0);
}

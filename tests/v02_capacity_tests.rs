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
use qubit_operation::ResourceScopeOptions;
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
    assert!(matches!(a.wait_outcome().unwrap(), OperationOutcome::Succeeded(1)));
    assert!(matches!(b.wait_outcome().unwrap(), OperationOutcome::Succeeded(2)));
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

    let mut waiting = Box::pin(async_manager.submit(OperationOptions::default(), |_| async { Ok::<_, ()>(()) }));
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(waiting.as_mut().poll(&mut context), std::task::Poll::Pending));

    release_tx.send(()).unwrap();
    let submitted = block_on(waiting);
    assert!(submitted.is_ok());
    assert!(matches!(
        block_on(held.wait_outcome()).unwrap(),
        OperationOutcome::Succeeded(())
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
    let mut child = child_handle_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let child_observer = child.observer();
    let before_release = child_observer.snapshot();
    assert!(
        before_release.stop.is_none(),
        "a Busy rejection must not stop the accepted child: {before_release:?}"
    );
    release_tx.send(()).unwrap();
    assert!(matches!(child.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
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
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(shutdown_report.snapshot.active_operations, 0);
}

#[test]
fn test_manager_resource_limit_is_inherited_by_default_root_and_can_be_overridden() {
    let manager = OperationManager::new(ManagerOptions {
        scope_limits: ScopeLimits {
            resources: 2,
            ..ScopeLimits::default()
        },
        ..ManagerOptions::default()
    })
    .unwrap();

    let mut inherited = manager
        .try_submit(OperationOptions::default(), |scope| {
            let _first = scope.manage(1usize).unwrap();
            let _second = scope.manage(2usize).unwrap();
            assert!(scope.manage(3usize).is_err());
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        inherited.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    inherited.wait_settlement().unwrap();

    let mut three = manager
        .try_submit(
            OperationOptions {
                limits: ScopeLimits {
                    resources: 3,
                    ..ScopeLimits::default()
                },
                ..OperationOptions::default()
            },
            |scope| {
                let _first = scope.manage(1usize).unwrap();
                let _second = scope.manage(2usize).unwrap();
                let _third = scope.manage(3usize).unwrap();
                assert!(scope.manage(4usize).is_err());
                Ok::<_, ()>(())
            },
        )
        .unwrap();
    assert!(matches!(three.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
    three.wait_settlement().unwrap();

    let mut one = manager
        .try_submit(
            OperationOptions {
                limits: ScopeLimits {
                    resources: 1,
                    ..ScopeLimits::default()
                },
                ..OperationOptions::default()
            },
            |scope| {
                let _first = scope.manage(1usize).unwrap();
                assert!(scope.manage(2usize).is_err());
                Ok::<_, ()>(())
            },
        )
        .unwrap();
    assert!(matches!(one.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
    one.wait_settlement().unwrap();
}

#[test]
fn test_child_resource_request_is_clamped_by_parent_and_capacity_is_reused_after_settlement() {
    let manager = OperationManager::new(ManagerOptions {
        scope_limits: ScopeLimits {
            resources: 2,
            ..ScopeLimits::default()
        },
        ..ManagerOptions::default()
    })
    .unwrap();
    let mut parent = manager
        .try_submit(
            OperationOptions {
                limits: ScopeLimits {
                    resources: 2,
                    ..ScopeLimits::default()
                },
                ..OperationOptions::default()
            },
            |scope| {
                let _parent_resource = scope.manage(1usize).unwrap();
                let mut child = scope
                    .manager()
                    .try_submit(
                        OperationOptions {
                            limits: ScopeLimits {
                                resources: 3,
                                ..ScopeLimits::default()
                            },
                            ..OperationOptions::default()
                        },
                        |child_scope| {
                            let _child_resource = child_scope.manage(2usize).unwrap();
                            assert!(child_scope.manage(3usize).is_err());
                            Ok::<_, ()>(())
                        },
                    )
                    .unwrap();
                assert!(matches!(child.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
                child.wait_settlement().unwrap();
                let mut reused = scope
                    .manager()
                    .try_submit(
                        OperationOptions {
                            limits: ScopeLimits {
                                resources: 3,
                                ..ScopeLimits::default()
                            },
                            ..OperationOptions::default()
                        },
                        |child_scope| {
                            let _resource = child_scope.manage(7usize).unwrap();
                            Ok::<_, ()>(())
                        },
                    )
                    .unwrap();
                assert!(matches!(
                    reused.wait_outcome().unwrap(),
                    OperationOutcome::Succeeded(())
                ));
                reused.wait_settlement().unwrap();
                Ok::<_, ()>(())
            },
        )
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    parent.wait_settlement().unwrap();

    let mut after_settlement = manager
        .try_submit(OperationOptions::default(), |scope| {
            let _first = scope.manage(4usize).unwrap();
            let _second = scope.manage(5usize).unwrap();
            assert!(scope.manage(6usize).is_err());
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        after_settlement.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    after_settlement.wait_settlement().unwrap();
}

#[test]
fn test_default_limits_are_replaced_as_a_whole_and_changed_limits_are_not_field_merged() {
    let manager = OperationManager::new(ManagerOptions {
        scope_limits: ScopeLimits {
            resources: 2,
            resource_scopes: 1,
            ..ScopeLimits::default()
        },
        ..ManagerOptions::default()
    })
    .unwrap();

    let mut default_limits = manager
        .try_submit(OperationOptions::default(), |scope| {
            let _first = scope.resource_scope(ResourceScopeOptions { capacity: 1 }).unwrap();
            assert!(scope.resource_scope(ResourceScopeOptions { capacity: 1 }).is_err());
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        default_limits.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    default_limits.wait_settlement().unwrap();

    let mut one_field_changed = manager
        .try_submit(
            OperationOptions {
                limits: ScopeLimits {
                    resources: 3,
                    ..ScopeLimits::default()
                },
                ..OperationOptions::default()
            },
            |scope| {
                let _first = scope.resource_scope(ResourceScopeOptions { capacity: 1 }).unwrap();
                let _second = scope.resource_scope(ResourceScopeOptions { capacity: 1 }).unwrap();
                Ok::<_, ()>(())
            },
        )
        .unwrap();
    assert!(matches!(
        one_field_changed.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    one_field_changed.wait_settlement().unwrap();
}

#[test]
fn test_default_child_limits_are_replaced_then_clamped_by_parent_limits() {
    let manager = OperationManager::new(ManagerOptions {
        scope_limits: ScopeLimits {
            resources: 2,
            ..ScopeLimits::default()
        },
        ..ManagerOptions::default()
    })
    .unwrap();
    let mut parent = manager
        .try_submit(
            OperationOptions {
                limits: ScopeLimits {
                    resources: 1,
                    ..ScopeLimits::default()
                },
                ..OperationOptions::default()
            },
            |scope| {
                let _parent_resource = scope.manage(1usize).unwrap();
                let mut child = scope
                    .manager()
                    .try_submit(OperationOptions::default(), |child_scope| {
                        assert!(child_scope.manage(2usize).is_err());
                        Ok::<_, ()>(())
                    })
                    .unwrap();
                assert!(matches!(child.wait_outcome().unwrap(), OperationOutcome::Succeeded(())));
                child.wait_settlement().unwrap();
                Ok::<_, ()>(())
            },
        )
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    parent.wait_settlement().unwrap();
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_operation::AsyncCleanup;
use qubit_operation::CleanupContext;
use qubit_operation::CleanupFuture;
use qubit_operation::CleanupOutcome;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ResourceRef;
use qubit_operation::ResourceScopeOptions;
use qubit_operation::ScopeLimits;
use qubit_operation::SettlementWait;
use qubit_operation::ShutdownMode;

struct Probe(Arc<AtomicUsize>);
impl Drop for Probe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct DropSignal(std::sync::mpsc::Sender<()>);
impl Drop for DropSignal {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

struct ChildPayload {
    _parent_ref: ResourceRef<Probe>,
}
struct GatedCleanup(crate::support::Gate);

impl AsyncCleanup<ChildPayload> for GatedCleanup {
    fn cleanup(self: Arc<Self>, resource: ChildPayload, _context: CleanupContext) -> CleanupFuture<ChildPayload> {
        let gate = self.0.clone();
        Box::pin(async move {
            gate.wait().await;
            drop(resource);
            CleanupOutcome::Released
        })
    }
}

impl AsyncCleanup<usize> for GatedCleanup {
    fn cleanup(self: Arc<Self>, resource: usize, _context: CleanupContext) -> CleanupFuture<usize> {
        let gate = self.0.clone();
        Box::pin(async move {
            gate.wait().await;
            let _ = resource;
            CleanupOutcome::Released
        })
    }
}

mod support;

#[test]
fn test_open_zero_can_get_again_then_settle_releases_once() {
    let drops = Arc::new(AtomicUsize::new(0));
    let body_drops = Arc::clone(&drops);
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let (key, resource_id) = {
                let input = scope.manage(Probe(Arc::clone(&body_drops))).map_err(|_| "manage")?;
                (input.key(), input.id())
            };
            assert_eq!(body_drops.load(Ordering::SeqCst), 0);
            let again = scope.get_resource(key).map_err(|_| "lookup")?;
            let copied_key_lookup = scope.get_resource(key).map_err(|_| "copied key lookup")?;
            assert_eq!(again.id(), resource_id);
            assert_eq!(copied_key_lookup.id(), resource_id);
            assert_eq!(body_drops.load(Ordering::SeqCst), 0);
            drop(again);
            drop(copied_key_lookup);
            Ok::<_, &'static str>(())
        })
        .unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert!(operation.wait_settlement().unwrap().all_released());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(shutdown_report.snapshot.cleaning_resources, 0);
}

#[test]
fn test_resource_ref_shares_mutex_protected_value() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut operation = manager
        .try_submit(OperationOptions::default(), |scope| {
            let value = scope.manage(std::sync::Mutex::new(1usize)).unwrap();
            let clone = value.clone();
            *clone.lock().unwrap() += 1;
            Ok::<_, ()>(*value.lock().unwrap())
        })
        .unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(2)
    ));
    operation.wait_settlement().unwrap();
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(shutdown_report.snapshot.cleaning_resources, 0);
}

#[test]
fn test_operation_scope_limits_the_number_of_attached_resource_scopes() {
    let options = ManagerOptions {
        scope_limits: ScopeLimits {
            resource_scopes: 1,
            ..ScopeLimits::default()
        },
        ..ManagerOptions::default()
    };
    let manager = OperationManager::new(options).unwrap();
    let mut operation = manager
        .try_submit(OperationOptions::default(), |scope| {
            let _first = scope.resource_scope(ResourceScopeOptions { capacity: 1 }).unwrap();
            assert!(matches!(
                scope.resource_scope(ResourceScopeOptions { capacity: 1 }),
                Err(qubit_operation::ResourceScopeError::CapacityExceeded)
            ));
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    operation.wait_settlement().unwrap();
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(shutdown_report.snapshot.cleaning_resources, 0);
}

#[test]
fn test_moving_scope_out_of_body_does_not_extend_its_authority() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let saved = Arc::new(std::sync::Mutex::new(None));
    let saved_in_body = Arc::clone(&saved);
    let mut operation = manager
        .try_submit(OperationOptions::default(), move |scope| {
            *saved_in_body.lock().unwrap() = Some(scope);
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    operation.wait_settlement().unwrap();
    let scope = saved.lock().unwrap().take().unwrap();
    let error = match scope.manage(7usize) {
        Ok(_) => panic!("ended scope unexpectedly accepted a resource"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), qubit_operation::ResourceManageErrorKind::ScopeEnded);
    let rejected = scope
        .manager()
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
    assert!(matches!(
        rejected,
        Err(error) if error.reason() == qubit_operation::AdmissionError::ScopeEnded
    ));
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(shutdown_report.snapshot.cleaning_resources, 0);
}

#[test]
fn test_child_cleanup_keeps_parent_resource_until_its_last_parent_ref_is_released() {
    let drop_count = Arc::new(AtomicUsize::new(0));
    let drops = Arc::clone(&drop_count);
    let probe = Probe(Arc::clone(&drop_count));
    let gate = support::Gate::new();
    let cleanup = Arc::new(GatedCleanup(gate.clone()));
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let parent_ref = scope.manage(probe).unwrap();
            let child_ref = parent_ref.clone();
            drop(parent_ref);
            let child = scope
                .manager()
                .try_submit(OperationOptions::default(), move |child_scope| {
                    let child_resources = child_scope
                        .resource_scope(qubit_operation::ResourceScopeOptions { capacity: 1 })
                        .unwrap();
                    let child = child_resources
                        .manage_with(ChildPayload { _parent_ref: child_ref }, cleanup)
                        .unwrap();
                    drop(child);
                    child_resources.end();
                    Ok::<_, ()>(())
                })
                .unwrap();
            let _child_observer = child.into_observer();
            Ok::<_, ()>(())
        })
        .unwrap();

    gate.wait_started();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "child cleanup still owns the parent reference"
    );
    assert!(matches!(
        parent.wait_settlement_for(Duration::ZERO).unwrap(),
        SettlementWait::Pending(_)
    ));
    gate.release();
    assert!(parent.wait_settlement().unwrap().all_released());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(shutdown_report.snapshot.cleaning_resources, 0);
}

#[test]
fn test_unrelated_child_cleanup_does_not_delay_parent_resource_release() {
    let (drop_tx, drop_rx) = std::sync::mpsc::channel();
    let probe = DropSignal(drop_tx);
    let gate = support::Gate::new();
    let cleanup = Arc::new(GatedCleanup(gate.clone()));
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let parent_ref = scope.manage(probe).unwrap();
            drop(parent_ref);
            let child = scope
                .manager()
                .try_submit(OperationOptions::default(), move |child_scope| {
                    let child_resources = child_scope
                        .resource_scope(qubit_operation::ResourceScopeOptions { capacity: 1 })
                        .unwrap();
                    let child = child_resources.manage_with(7usize, cleanup).unwrap();
                    drop(child);
                    child_resources.end();
                    Ok::<_, ()>(())
                })
                .unwrap();
            let _child_observer = child.into_observer();
            Ok::<_, ()>(())
        })
        .unwrap();

    gate.wait_started();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    drop_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("parent resource should be released while unrelated child cleanup is blocked");
    assert!(matches!(
        parent.wait_settlement_for(Duration::ZERO).unwrap(),
        SettlementWait::Pending(_)
    ));
    gate.release();
    assert!(parent.wait_settlement().unwrap().all_released());
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(shutdown_report.snapshot.cleaning_resources, 0);
}

#[test]
fn test_open_scope_reacquires_resource_by_typed_key() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut operation = manager
        .try_submit(OperationOptions::default(), |scope| {
            let resource_scope = scope.resource_scope(ResourceScopeOptions { capacity: 2 }).unwrap();
            let first = resource_scope.manage(String::from("ready")).unwrap();
            let key = first.key();
            let second = resource_scope.get_resource(key).unwrap();
            assert_eq!(&*second, "ready");
            assert_eq!(first.id(), second.id());
            resource_scope.end();
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    operation.wait_settlement().unwrap();
    drop(manager.request_shutdown(ShutdownMode::Drain).wait().unwrap());
}

#[test]
fn test_ended_scope_keeps_live_reference_readable_and_disallows_get() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut operation = manager
        .try_submit(OperationOptions::default(), |scope| {
            let resource_scope = scope.resource_scope(ResourceScopeOptions { capacity: 2 }).unwrap();
            let resource = resource_scope.manage(18_u32).unwrap();
            let key = resource.key();
            resource_scope.end();
            assert_eq!(*resource, 18);
            assert!(matches!(
                resource_scope.get_resource(key),
                Err(qubit_operation::ResourceLookupError::ScopeEnded)
            ));
            drop(resource);
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    operation.wait_settlement().unwrap();
    drop(manager.request_shutdown(ShutdownMode::Drain).wait().unwrap());
}

#[test]
fn test_resources_in_distinct_scopes_have_distinct_ids() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut operation = manager
        .try_submit(OperationOptions::default(), |scope| {
            let operation_resource = scope.manage(String::from("operation")).unwrap();
            let first_scope = scope.resource_scope(ResourceScopeOptions { capacity: 1 }).unwrap();
            let first = first_scope.manage(String::from("first")).unwrap();
            let second_scope = scope.resource_scope(ResourceScopeOptions { capacity: 1 }).unwrap();
            let second = second_scope.manage(String::from("second")).unwrap();

            assert_ne!(operation_resource.id(), first.id());
            assert_ne!(operation_resource.id(), second.id());
            assert_ne!(first.id(), second.id());
            assert_eq!(first.id(), first.clone().id());

            first_scope.end();
            second_scope.end();
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    operation.wait_settlement().unwrap();
    drop(manager.request_shutdown(ShutdownMode::Drain).wait().unwrap());
}

#[test]
fn test_resource_ids_are_unique_across_operations_of_one_manager() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut first = manager
        .try_submit(OperationOptions::default(), |scope| {
            Ok::<_, ()>(scope.manage(String::from("first")).unwrap().id())
        })
        .unwrap();
    let first_id = match first.wait_outcome().unwrap() {
        OperationOutcome::Succeeded(id) => id,
        outcome => panic!("unexpected first operation outcome: {outcome:?}"),
    };
    first.wait_settlement().unwrap();

    let mut second = manager
        .try_submit(OperationOptions::default(), |scope| {
            Ok::<_, ()>(scope.manage(String::from("second")).unwrap().id())
        })
        .unwrap();
    let second_id = match second.wait_outcome().unwrap() {
        OperationOutcome::Succeeded(id) => id,
        outcome => panic!("unexpected second operation outcome: {outcome:?}"),
    };
    second.wait_settlement().unwrap();
    assert_ne!(first_id, second_id);
    drop(manager.request_shutdown(ShutdownMode::Drain).wait().unwrap());
}

#[test]
fn test_concurrent_resource_registrations_receive_distinct_ids() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut operation = manager
        .try_submit(OperationOptions::default(), |scope| {
            let scope = &scope;
            let ids = std::thread::scope(|threads| {
                let workers = (0..8)
                    .map(|worker| {
                        threads.spawn(move || {
                            (0..16)
                                .map(|resource| scope.manage(format!("{worker}-{resource}")).unwrap().id())
                                .collect::<Vec<_>>()
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .flat_map(|worker| worker.join().unwrap())
                    .collect::<Vec<_>>()
            });
            let unique = ids.iter().copied().collect::<HashSet<_>>();
            assert_eq!(ids.len(), 8 * 16);
            assert_eq!(unique.len(), ids.len());
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(())
    ));
    operation.wait_settlement().unwrap();
    drop(manager.request_shutdown(ShutdownMode::Drain).wait().unwrap());
}

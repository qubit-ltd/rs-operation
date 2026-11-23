// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use qubit_operation::GroupBindingError;
use qubit_operation::GroupOptions;
use qubit_operation::ManagerCreateError;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;

#[test]
fn test_with_group_returns_same_constraint_view_and_deduplicates() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let bound = manager.with_group(&group).unwrap();
    let repeated = bound.with_group(&group).unwrap();
    assert_eq!(group.snapshot().active_operations, 0);
    assert!(bound.as_async().as_sync().with_group(&group).is_ok());
    drop(repeated);
    drop(bound);
    group.close_admission();
    assert!(matches!(manager.with_group(&group), Err(GroupBindingError::Closed)));
}

#[test]
fn test_unsettled_operation_count_ignores_unobserved_settled_outcome() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let async_group = group.as_async();
    assert_eq!(group.unsettled_operation_count(), 0);
    assert_eq!(async_group.unsettled_operation_count(), 0);

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let mut operation = group
        .try_submit(Default::default(), move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(17usize)
        })
        .unwrap();
    started_rx.recv().unwrap();
    assert_eq!(group.unsettled_operation_count(), 1);
    assert_eq!(async_group.unsettled_operation_count(), 1);

    release_tx.send(()).unwrap();
    operation.wait_settlement().unwrap();
    assert_eq!(group.unsettled_operation_count(), 0);
    assert_eq!(async_group.unsettled_operation_count(), 0);
    assert_eq!(group.snapshot().active_operations, 1);
    assert!(matches!(
        operation.wait_outcome().unwrap(),
        qubit_operation::OperationOutcome::Succeeded(17)
    ));
}

#[test]
fn test_unsettled_operation_count_tracks_borrowed_child_separately() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let (child_started_tx, child_started_rx) = std::sync::mpsc::channel();
    let (release_child_tx, release_child_rx) = std::sync::mpsc::channel();
    let (child_settled_tx, child_settled_rx) = std::sync::mpsc::channel();
    let (release_parent_tx, release_parent_rx) = std::sync::mpsc::channel();
    let mut parent = group
        .try_submit(Default::default(), move |scope| {
            let child = scope
                .manager()
                .try_submit(Default::default(), move |_| {
                    child_started_tx.send(()).unwrap();
                    release_child_rx.recv().unwrap();
                    Ok::<_, ()>(23usize)
                })
                .unwrap();
            child.wait_settlement().unwrap();
            child_settled_tx.send(()).unwrap();
            release_parent_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();

    child_started_rx.recv().unwrap();
    assert_eq!(group.unsettled_operation_count(), 2);
    release_child_tx.send(()).unwrap();
    child_settled_rx.recv().unwrap();
    assert_eq!(group.unsettled_operation_count(), 1);

    release_parent_tx.send(()).unwrap();
    parent.wait_settlement().unwrap();
    assert_eq!(group.unsettled_operation_count(), 0);
    assert!(parent.wait_outcome().is_ok());
}

#[test]
fn test_distinct_groups_both_track_a_bound_operation() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let first = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let bound = manager.with_group(&first).unwrap().with_group(&second).unwrap();
    let mut operation = bound.try_submit(Default::default(), |_| Ok::<_, ()>(())).unwrap();
    assert!(operation.wait_outcome().is_ok());
    operation.wait_settlement().unwrap();
    assert_eq!(first.snapshot().active_operations, 0);
    assert_eq!(second.snapshot().active_operations, 0);
}

#[test]
fn test_group_binding_limit_uses_manager_configuration() {
    let options = ManagerOptions {
        max_groups_per_operation: 1,
        ..ManagerOptions::default()
    };
    let manager = OperationManager::new(options).unwrap();
    let first = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let bound = manager.with_group(&first).unwrap();
    assert!(matches!(
        bound.with_group(&second),
        Err(GroupBindingError::CapacityExceeded)
    ));
}

#[test]
fn test_zero_group_binding_limit_is_rejected() {
    let options = ManagerOptions {
        max_groups_per_operation: 0,
        ..ManagerOptions::default()
    };
    assert!(matches!(
        OperationManager::new(options),
        Err(ManagerCreateError::InvalidOptions {
            field: "max_groups_per_operation"
        })
    ));
}

#[test]
fn test_closing_any_bound_group_rejects_new_submission() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let first = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let bound = manager.with_group(&first).unwrap().with_group(&second).unwrap();
    second.close_admission();
    let rejected = bound.try_submit(Default::default(), |_| Ok::<_, ()>(()));
    assert!(matches!(rejected, Err(error) if error.reason() == qubit_operation::AdmissionError::Closed));
}

#[test]
fn test_parent_bound_view_inherits_and_adds_group_constraints() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let first = manager.group(GroupOptions { capacity: 4 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 4 }).unwrap();
    let second_for_child = second.clone();
    let mut parent = first
        .try_submit(Default::default(), move |scope| {
            let bound = scope.manager().with_group(&second_for_child).unwrap();
            let mut child = bound.try_submit(Default::default(), |_| Ok::<_, ()>(9usize)).unwrap();
            assert!(matches!(
                child.wait_outcome().unwrap(),
                qubit_operation::OperationOutcome::Succeeded(9)
            ));
            child.wait_settlement().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(parent.wait_outcome().is_ok());
    parent.wait_settlement().unwrap();
    assert_eq!(first.snapshot().active_operations, 0);
    assert_eq!(second.snapshot().active_operations, 0);
}

#[test]
fn test_default_group_limit_accepts_sixteen_and_deduplicates_before_rejecting_seventeen() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let groups: Vec<_> = (0..17)
        .map(|_| manager.group(GroupOptions { capacity: 2 }).unwrap())
        .collect();
    let bound = groups[..16]
        .iter()
        .fold(manager.clone(), |view, group| view.with_group(group).unwrap());
    let duplicate = bound.with_group(&groups[0]).unwrap();
    assert!(matches!(
        duplicate.with_group(&groups[16]),
        Err(GroupBindingError::CapacityExceeded)
    ));
}

#[test]
fn test_every_bound_group_capacity_participates_in_admission() {
    use std::sync::mpsc;
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let first = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let bound = manager.with_group(&first).unwrap().with_group(&second).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut held = bound
        .try_submit(Default::default(), move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    started_rx.recv().unwrap();
    let rejected = bound.try_submit(Default::default(), |_| Ok::<_, ()>(()));
    assert!(matches!(rejected, Err(error) if error.reason() == qubit_operation::AdmissionError::Busy));
    release_tx.send(()).unwrap();
    let _ = held.wait_outcome().unwrap();
    held.wait_settlement().unwrap();
}

#[test]
fn test_closing_any_group_stops_an_already_admitted_operation() {
    use std::sync::mpsc;

    use qubit_operation::OperationOutcome;
    use qubit_operation::ShutdownMode;
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let first = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let bound = manager.with_group(&first).unwrap().with_group(&second).unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut held = bound
        .try_submit(Default::default(), move |_| {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    started_rx.recv().unwrap();
    let _shutdown = second.request_shutdown(ShutdownMode::Cancel);
    release_tx.send(()).unwrap();
    assert!(matches!(held.wait_outcome().unwrap(), OperationOutcome::Stopped(_)));
    held.wait_settlement().unwrap();
}

#[test]
fn test_effect_records_obey_inherited_quota_and_report_overflow() {
    use qubit_operation::DiagnosticSummary;
    use qubit_operation::EffectFact;
    use qubit_operation::EffectRecord;
    use qubit_operation::EffectRecordError;
    let mut options = ManagerOptions::default();
    options.scope_limits.effect_records = 1;
    let manager = OperationManager::new(options).unwrap();
    let mut handle = manager
        .try_submit(Default::default(), |scope| {
            let make_record = || EffectRecord {
                operation_id: scope.id(),
                resource_id: None,
                fact: EffectFact::Known,
                description: DiagnosticSummary::new("effect"),
            };
            scope.record_effect(make_record()).unwrap();
            assert_eq!(
                scope.record_effect(make_record()),
                Err(EffectRecordError::CapacityExceeded)
            );
            Ok::<_, ()>(())
        })
        .unwrap();
    let _ = handle.wait_outcome().unwrap();
    let settlement = handle.wait_settlement().unwrap();
    assert_eq!(settlement.effects.len(), 1);
    assert_eq!(settlement.effects_total, 2);
    assert_eq!(settlement.effect_overflow, 1);
}

#[test]
fn test_stopped_scope_rejects_effects_and_stop_status_distinguishes_finished() {
    use std::sync::Arc;
    use std::sync::Mutex;

    use qubit_operation::DiagnosticSummary;
    use qubit_operation::EffectFact;
    use qubit_operation::EffectRecord;
    use qubit_operation::EffectRecordError;
    use qubit_operation::StopReason;
    use qubit_operation::StopRequestStatus;
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let saved = Arc::new(Mutex::new(None));
    let saved_body = Arc::clone(&saved);
    let mut handle = manager
        .try_submit(Default::default(), move |scope| {
            assert_eq!(
                scope.request_stop(StopReason::UserCancelled),
                StopRequestStatus::Accepted
            );
            let record = EffectRecord {
                operation_id: scope.id(),
                resource_id: None,
                fact: EffectFact::Unknown,
                description: DiagnosticSummary::new("maybe"),
            };
            assert_eq!(scope.record_effect(record), Err(EffectRecordError::Stopped));
            *saved_body.lock().unwrap() = Some(scope);
            Ok::<_, ()>(())
        })
        .unwrap();
    let _ = handle.wait_outcome().unwrap();
    handle.wait_settlement().unwrap();
    let scope = saved.lock().unwrap().take().unwrap();
    assert_eq!(
        scope.request_stop(StopReason::Timeout),
        StopRequestStatus::AlreadyFinished
    );
    let record = EffectRecord {
        operation_id: scope.id(),
        resource_id: None,
        fact: EffectFact::Unknown,
        description: DiagnosticSummary::new("late"),
    };
    assert_eq!(scope.record_effect(record), Err(EffectRecordError::ScopeEnded));
}

#[test]
fn test_parent_stop_reaches_operation_submitted_through_additional_group_view() {
    use std::sync::mpsc;

    use qubit_operation::OperationOutcome;
    use qubit_operation::StopReason;
    use qubit_operation::StopRequestStatus;
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let parent_group = manager.group(GroupOptions { capacity: 3 }).unwrap();
    let child_group = manager.group(GroupOptions { capacity: 3 }).unwrap();
    let child_group_for_body = child_group.clone();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let mut parent = parent_group
        .try_submit(Default::default(), move |scope| {
            let bound = scope.manager().with_group(&child_group_for_body).unwrap();
            let mut child = bound
                .try_submit(Default::default(), move |_| {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    Ok::<_, ()>(())
                })
                .unwrap();
            started_rx.recv().unwrap();
            assert_eq!(
                scope.request_stop(StopReason::UserCancelled),
                StopRequestStatus::Accepted
            );
            release_tx.send(()).unwrap();
            assert!(matches!(child.wait_outcome().unwrap(), OperationOutcome::Stopped(_)));
            child.wait_settlement().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::ChildFailed(_)
    ));
    parent.wait_settlement().unwrap();
}

#[test]
fn test_service_scope_registered_with_every_inherited_group() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let first = manager.group(GroupOptions { capacity: 3 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 3 }).unwrap();
    let bound = manager.with_group(&first).unwrap().with_group(&second).unwrap();
    let service_group = bound.group(GroupOptions { capacity: 3 }).unwrap();
    let service = service_group
        .resource_scope(qubit_operation::ServiceResourceOptions { capacity: 1 })
        .unwrap();
    assert_eq!(first.snapshot().service_resource_scopes, 1);
    assert_eq!(second.snapshot().service_resource_scopes, 1);
    assert_eq!(service_group.snapshot().service_resource_scopes, 1);
    service.close();
}

#[test]
fn test_execution_context_detects_every_bound_group_for_deadlock_checks() {
    use qubit_operation::ObservationError;
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let first = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let second = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let second_for_body = second.clone();
    let bound = manager.with_group(&first).unwrap().with_group(&second).unwrap();
    let mut operation = bound
        .try_submit(Default::default(), move |_| {
            let shutdown = second_for_body.request_shutdown(qubit_operation::ShutdownMode::Drain);
            assert!(matches!(shutdown.wait(), Err(ObservationError::WouldDeadlock)));
            Ok::<_, ()>(())
        })
        .unwrap();
    let _ = operation.wait_outcome().unwrap();
    operation.wait_settlement().unwrap();
}

#[test]
fn test_resource_scope_reservations_commit_values_and_preserve_unreserved_owner() {
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use qubit_operation::AsyncCleanup;
    use qubit_operation::CleanupContext;
    use qubit_operation::CleanupFuture;
    use qubit_operation::CleanupOutcome;
    use qubit_operation::OperationOptions;
    use qubit_operation::ResourceScopeOptions;

    struct Release;
    impl AsyncCleanup<u32> for Release {
        fn cleanup(self: Arc<Self>, resource: u32, _context: CleanupContext) -> CleanupFuture<u32> {
            Box::pin(async move {
                let _ = resource;
                CleanupOutcome::Released
            })
        }
    }
    struct DropProbe(Arc<AtomicUsize>);
    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicUsize::new(0));
    let dropped_in_body = Arc::clone(&dropped);
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut handle = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let resources = scope.resource_scope(ResourceScopeOptions { capacity: 2 }).unwrap();
            let first = resources
                .reserve_resource()
                .unwrap()
                .commit(1u32)
                .unwrap_or_else(|_| panic!("commit failed"));
            let second = resources
                .reserve_resource()
                .unwrap()
                .commit_with(2u32, Arc::new(Release))
                .unwrap_or_else(|_| panic!("custom cleanup commit failed"));
            let probe = DropProbe(Arc::clone(&dropped_in_body));
            assert!(matches!(
                resources.reserve_resource(),
                Err(qubit_operation::ResourceCapacityError::CapacityExceeded)
            ));
            assert_eq!(
                dropped_in_body.load(Ordering::SeqCst),
                0,
                "failed reservation consumed an unrelated owner"
            );
            drop(probe);
            drop(first);
            drop(second);
            resources.end();
            Ok::<_, ()>(())
        })
        .unwrap();
    let _ = handle.wait_outcome().unwrap();
    handle.wait_settlement().unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[test]
fn test_group_binding_rejects_another_manager_and_does_not_mutate_original_view() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let other_manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
    assert!(matches!(
        other_manager.with_group(&group),
        Err(GroupBindingError::WrongManager)
    ));
    let _bound = manager.with_group(&group).unwrap();
    group.close_admission();
    let mut unbound_operation = manager.try_submit(Default::default(), |_| Ok::<_, ()>(())).unwrap();
    let _ = unbound_operation.wait_outcome().unwrap();
    unbound_operation.wait_settlement().unwrap();
}

#[test]
fn test_inherited_parent_group_counts_toward_group_binding_limit() {
    let options = ManagerOptions {
        max_groups_per_operation: 1,
        ..ManagerOptions::default()
    };
    let manager = OperationManager::new(options).unwrap();
    let parent_group = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let second_group = manager.group(GroupOptions { capacity: 2 }).unwrap();
    let second_for_body = second_group.clone();
    let mut parent = parent_group
        .try_submit(Default::default(), move |scope| {
            assert!(matches!(
                scope.manager().with_group(&second_for_body),
                Err(GroupBindingError::CapacityExceeded)
            ));
            Ok::<_, ()>(())
        })
        .unwrap();
    let _ = parent.wait_outcome().unwrap();
    parent.wait_settlement().unwrap();
}

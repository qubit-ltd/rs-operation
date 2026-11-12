// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_operation::protocol::CleanupContext;
use qubit_operation::protocol::OwnedCleanup;
use qubit_operation::report::CleanupOutcome;
use qubit_operation::report::OperationId;
use qubit_operation::report::ResourceId;
use qubit_operation::resource::RegisterErrorKind;
use qubit_operation::resource::ResourceRegistry;

struct DropOwner(Arc<AtomicUsize>);

impl Drop for DropOwner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

impl OwnedCleanup for DropOwner {
    fn begin(
        self: Box<Self>,
        _context: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        Box::pin(async move {
            drop(self);
            CleanupOutcome::Cleaned
        })
    }
}

fn owner(drop_count: &Arc<AtomicUsize>) -> Box<dyn OwnedCleanup> {
    Box::new(DropOwner(Arc::clone(drop_count)))
}

#[test]
fn test_resource_owner_waits_for_last_use_lease_before_cleanup() {
    let registry = ResourceRegistry::new(OperationId::new(1), 2);
    let drop_count = Arc::new(AtomicUsize::new(0));
    let resource = registry.register(owner(&drop_count)).unwrap();
    let first_use = resource.acquire_use().unwrap();
    let second_use = resource.acquire_use().unwrap();
    resource.close_for_use().unwrap();

    assert!(resource.acquire_use().is_err());
    assert!(registry.take_ready_cleanup().is_none());
    drop(first_use);
    assert!(registry.take_ready_cleanup().is_none());
    drop(second_use);

    let ready = registry.take_ready_cleanup().unwrap();
    assert_eq!(ready.resource_id(), resource.id());
    assert_eq!(drop_count.load(Ordering::SeqCst), 0);
}

#[test]
fn test_cleanup_dependency_requires_release_and_rejects_cycles() {
    let registry = ResourceRegistry::new(OperationId::new(2), 4);
    let drop_count = Arc::new(AtomicUsize::new(0));
    let resource_a = registry.register(owner(&drop_count)).unwrap();
    let resource_b = registry.register(owner(&drop_count)).unwrap();

    resource_a.cleanup_after(&resource_b).unwrap();
    assert!(resource_a.cleanup_after(&resource_a).is_err());
    assert!(resource_b.cleanup_after(&resource_a).is_err());
    resource_a.close_for_use().unwrap();
    resource_b.close_for_use().unwrap();

    let ready_b = registry.take_ready_cleanup().unwrap();
    assert_eq!(ready_b.resource_id(), resource_b.id());
    assert!(registry.take_ready_cleanup().is_none());
    registry.confirm_released(resource_b.id()).unwrap();
    let ready_a = registry.take_ready_cleanup().unwrap();
    assert_eq!(ready_a.resource_id(), resource_a.id());
}

#[test]
fn test_cross_registry_dependency_is_rejected_without_accepting_owner() {
    let first = ResourceRegistry::new(OperationId::new(3), 2);
    let second = ResourceRegistry::new(OperationId::new(3), 2);
    let drop_count = Arc::new(AtomicUsize::new(0));
    let resource = first.register(owner(&drop_count)).unwrap();
    let dependency = second.register(owner(&drop_count)).unwrap();

    assert!(resource.cleanup_after(&dependency).is_err());
    assert_eq!(drop_count.load(Ordering::SeqCst), 0);
    assert!(first.contains(resource.id()));
}

#[test]
fn test_registration_capacity_failure_returns_owner_without_dropping_it() {
    let registry = ResourceRegistry::new(OperationId::new(7), 1);
    let drop_count = Arc::new(AtomicUsize::new(0));
    let _accepted = registry.register(owner(&drop_count)).unwrap();
    let error = registry.register(owner(&drop_count)).unwrap_err();

    assert_eq!(error.kind(), RegisterErrorKind::CapacityExceeded);
    assert_eq!(drop_count.load(Ordering::SeqCst), 0);
    drop(error.into_owner());
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_failed_transfer_keeps_owner_registered_at_source() {
    let source = ResourceRegistry::new(OperationId::new(5), 2);
    let destination = ResourceRegistry::new(OperationId::new(6), 1);
    let drop_count = Arc::new(AtomicUsize::new(0));
    let resource = source.register(owner(&drop_count)).unwrap();
    let dependency = source.register(owner(&drop_count)).unwrap();
    resource.cleanup_after(&dependency).unwrap();
    let resource_id = resource.id();
    let dependency_id = dependency.id();

    assert!(source.transfer_to(&resource, &destination).is_err());
    assert!(source.contains(resource_id));
    assert!(source.contains(dependency_id));
    assert!(!destination.contains(ResourceId::new(OperationId::new(6), 0, 0)));
    assert_eq!(drop_count.load(Ordering::SeqCst), 0);
}

#[test]
fn test_transfer_moves_dependency_group_after_reserving_all_target_slots() {
    let source = ResourceRegistry::new(OperationId::new(8), 2);
    let destination = ResourceRegistry::new(OperationId::new(9), 2);
    let drop_count = Arc::new(AtomicUsize::new(0));
    let resource_a = source.register(owner(&drop_count)).unwrap();
    let resource_b = source.register(owner(&drop_count)).unwrap();
    let use_a = resource_a.acquire_use().unwrap();
    let old_a_id = resource_a.id();
    let old_b_id = resource_b.id();
    resource_a.cleanup_after(&resource_b).unwrap();

    source.transfer_to(&resource_a, &destination).unwrap();

    assert!(!source.contains(old_a_id));
    assert!(!source.contains(old_b_id));
    assert!(destination.contains(resource_a.id()));
    assert!(destination.contains(resource_b.id()));
    assert_eq!(resource_a.id().operation, OperationId::new(9));
    resource_a.close_for_use().unwrap();
    resource_b.close_for_use().unwrap();
    assert_eq!(destination.take_ready_cleanup().unwrap().resource_id(), resource_b.id());
    destination.confirm_released(resource_b.id()).unwrap();
    assert!(destination.take_ready_cleanup().is_none());
    drop(use_a);
    assert_eq!(destination.take_ready_cleanup().unwrap().resource_id(), resource_a.id());
}

#[test]
fn test_cleanup_owner_transfer_needs_receiver_receipt_before_source_releases_it() {
    let source = ResourceRegistry::new(OperationId::new(10), 1);
    let receiver = ResourceRegistry::new(OperationId::new(11), 1);
    let drop_count = Arc::new(AtomicUsize::new(0));
    let resource = source.register(owner(&drop_count)).unwrap();
    let source_id = resource.id();
    resource.close_for_use().unwrap();
    let ready = source.take_ready_cleanup().unwrap();
    let (ready_id, owner) = ready.into_parts();
    assert_eq!(ready_id, source_id);

    let (received_id, receipt) = receiver.accept_cleanup_transfer(source_id, owner).unwrap();
    assert_eq!(receipt.resource(), source_id);
    assert!(source.contains(source_id));
    source.confirm_transferred(source_id, receipt).unwrap();
    assert!(source.contains(source_id));
    assert!(receiver.contains(received_id));

    let received = receiver.take_ready_cleanup().unwrap();
    assert_eq!(received.resource_id(), received_id);
    let (received_id, receiver_owner) = received.into_parts();
    drop(receiver_owner);
    let release = receiver.confirm_released_with_receipt(received_id).unwrap().unwrap();
    assert!(source.confirm_released(source_id).is_err());
    source.confirm_transferred_released(source_id, release).unwrap();
    assert!(!source.contains(source_id));
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_transferred_prerequisite_stays_blocked_until_external_release_confirmation() {
    let source = ResourceRegistry::new(OperationId::new(14), 2);
    let receiver = ResourceRegistry::new(OperationId::new(15), 1);
    let drop_count = Arc::new(AtomicUsize::new(0));
    let dependent = source.register(owner(&drop_count)).unwrap();
    let prerequisite = source.register(owner(&drop_count)).unwrap();
    let dependent_id = dependent.id();
    let prerequisite_id = prerequisite.id();
    dependent.cleanup_after(&prerequisite).unwrap();
    dependent.close_for_use().unwrap();
    prerequisite.close_for_use().unwrap();
    let ready = source.take_ready_cleanup().unwrap();
    assert_eq!(ready.resource_id(), prerequisite_id);
    let (_, transferred_owner) = ready.into_parts();

    let (received_id, receipt) = receiver
        .accept_cleanup_transfer(prerequisite_id, transferred_owner)
        .unwrap();
    assert!(receiver.contains(received_id));
    source.confirm_transferred(prerequisite_id, receipt).unwrap();
    assert!(source.take_ready_cleanup().is_none());
    assert!(source.contains(dependent_id));
    assert!(source.contains(prerequisite_id));

    let received = receiver.take_ready_cleanup().unwrap();
    let (received_id, receiver_owner) = received.into_parts();
    drop(receiver_owner);
    let release = receiver.confirm_released_with_receipt(received_id).unwrap().unwrap();
    assert!(source.confirm_released(prerequisite_id).is_err());

    let decoy = ResourceRegistry::new(OperationId::new(16), 2);
    let (_, decoy_acceptance) = decoy
        .accept_cleanup_transfer(prerequisite_id, owner(&drop_count))
        .unwrap();
    let decoy_id = decoy_acceptance.receiver();
    let decoy_ready = decoy.take_ready_cleanup().unwrap();
    let (_, decoy_owner) = decoy_ready.into_parts();
    drop(decoy_owner);
    let wrong_receiver_proof = decoy.confirm_released_with_receipt(decoy_id).unwrap().unwrap();
    assert!(
        source
            .confirm_transferred_released(prerequisite_id, wrong_receiver_proof)
            .is_err()
    );
    let (_, old_generation_acceptance) = decoy
        .accept_cleanup_transfer(
            ResourceId::new(
                prerequisite_id.operation,
                prerequisite_id.slot,
                prerequisite_id.generation.wrapping_sub(1),
            ),
            owner(&drop_count),
        )
        .unwrap();
    let old_generation_id = old_generation_acceptance.receiver();
    let old_ready = decoy.take_ready_cleanup().unwrap();
    let (_, old_owner) = old_ready.into_parts();
    drop(old_owner);
    let old_generation_proof = decoy.confirm_released_with_receipt(old_generation_id).unwrap().unwrap();
    assert!(
        source
            .confirm_transferred_released(prerequisite_id, old_generation_proof)
            .is_err()
    );
    assert!(source.contains(prerequisite_id));
    assert!(source.take_ready_cleanup().is_none());
    source.confirm_transferred_released(prerequisite_id, release).unwrap();
    let ready_dependent = source.take_ready_cleanup().unwrap();
    assert_eq!(ready_dependent.resource_id(), dependent_id);
    let (_, owner) = ready_dependent.into_parts();
    drop(owner);
    source.confirm_released(dependent_id).unwrap();
    assert_eq!(drop_count.load(Ordering::SeqCst), 4);
}

#[test]
fn test_rejected_cleanup_owner_transfer_can_be_restored_at_source() {
    let source = ResourceRegistry::new(OperationId::new(12), 1);
    let receiver = ResourceRegistry::new(OperationId::new(13), 0);
    let drop_count = Arc::new(AtomicUsize::new(0));
    let resource = source.register(owner(&drop_count)).unwrap();
    let resource_id = resource.id();
    resource.close_for_use().unwrap();
    let ready = source.take_ready_cleanup().unwrap();
    let (_, owner) = ready.into_parts();

    let error = receiver.accept_cleanup_transfer(resource_id, owner).unwrap_err();
    assert!(source.contains(resource_id));
    match source.restore_cleanup_owner(resource_id, error.into_owner()) {
        Ok(()) => {}
        Err((restore_error, _owner)) => panic!("owner restore failed: {restore_error:?}"),
    }
    let ready_again = source.take_ready_cleanup().unwrap();
    assert_eq!(ready_again.resource_id(), resource_id);
    let (_, owner) = ready_again.into_parts();
    drop(owner);
    source.confirm_released(resource_id).unwrap();
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
}

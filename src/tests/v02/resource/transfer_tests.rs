// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::ManagerId;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ResourceTransferError;
use crate::v02::resource::dependency::DependencyGraph;
use crate::v02::resource::transfer::TransferRegistry;

fn id(manager: ManagerId, slot: u64) -> ResourceId {
    ResourceId::new(manager, slot, 1)
}

#[test]
fn test_dependency_group_transfer_is_atomic_and_returns_receipt() {
    let manager = ManagerId::new_unique();
    let source_scope = ResourceScopeId::new_unique();
    let destination_scope = ResourceScopeId::new_unique();
    let dependencies = Arc::new(DependencyGraph::default());
    assert!(
        dependencies
            .add(id(manager, 2), source_scope, id(manager, 1), source_scope, Box::new(()))
            .is_ok()
    );
    let source = TransferRegistry::new(manager, source_scope, 2, Arc::clone(&dependencies));
    let destination = TransferRegistry::new(manager, destination_scope, 1, Arc::clone(&dependencies));
    source.insert(id(manager, 1), "parent").unwrap();
    source.insert(id(manager, 2), "child").unwrap();

    let error = source
        .transfer_group_to(&destination, &[id(manager, 1), id(manager, 2)])
        .unwrap_err();
    assert_eq!(error, ResourceTransferError::CapacityExceeded);
    assert_eq!(source.len(), 2);
    assert_eq!(destination.len(), 0);

    let destination = TransferRegistry::new(manager, destination_scope, 2, Arc::clone(&dependencies));
    let transfer = source
        .transfer_group_to(&destination, &[id(manager, 1), id(manager, 2)])
        .unwrap();
    assert_eq!(transfer.members.len(), 2);
    assert_eq!(transfer.receipt.resource(), id(manager, 1));
    assert_eq!(transfer.receipt.source_scope(), source_scope);
    assert_eq!(transfer.receipt.target_scope(), destination_scope);
    assert_eq!(source.len(), 0);
    assert_eq!(destination.len(), 2);
}

#[test]
fn test_incomplete_group_transfer_keeps_source_unchanged() {
    let manager = ManagerId::new_unique();
    let scope = ResourceScopeId::new_unique();
    let dependencies = Arc::new(DependencyGraph::default());
    assert!(
        dependencies
            .add(id(manager, 2), scope, id(manager, 1), scope, Box::new(()))
            .is_ok()
    );
    let source = TransferRegistry::new(manager, scope, 2, Arc::clone(&dependencies));
    let destination = TransferRegistry::new(manager, ResourceScopeId::new_unique(), 2, Arc::clone(&dependencies));
    source.insert(id(manager, 1), "only member").unwrap();
    assert_eq!(
        source.transfer_group_to(&destination, &[id(manager, 1), id(manager, 2)]),
        Err(ResourceTransferError::IncompleteDependencyGroup)
    );
    assert_eq!(source.len(), 1);
    assert_eq!(destination.len(), 0);
}

#[test]
fn test_cleanup_started_group_cannot_be_reopened_by_transfer() {
    let manager = ManagerId::new_unique();
    let scope = ResourceScopeId::new_unique();
    let dependencies = Arc::new(DependencyGraph::default());
    let resource = id(manager, 1);
    let source = TransferRegistry::new(manager, scope, 1, Arc::clone(&dependencies));
    let destination = TransferRegistry::new(manager, ResourceScopeId::new_unique(), 1, Arc::clone(&dependencies));
    source.insert(resource, "owner").unwrap();
    assert!(dependencies.try_start(resource));
    assert_eq!(
        source.transfer_group_to(&destination, &[resource]),
        Err(ResourceTransferError::CleanupStarted)
    );
    assert_eq!(source.len(), 1);
    assert_eq!(destination.len(), 0);
}

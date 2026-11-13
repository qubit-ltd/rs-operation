// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Recovery-key validation contracts for settlement owners.

use crate::report::OperationId;
use crate::report::RecoveryKey;
use crate::report::RecoveryOwner;
use crate::report::ResourceId;

#[test]
fn test_recovery_key_matches_resource_generation_and_nonce() {
    let id = ResourceId::new(OperationId::new(1), 4, 8);
    let owner = RecoveryOwner::new(&RecoveryKey::<String>::new(1, id, 13), String::from("owned"));
    let wrong = RecoveryKey::<String>::new(1, ResourceId::new(OperationId::new(1), 4, 7), 13);
    let owner = owner.take(wrong).unwrap_err();
    let wrong = RecoveryKey::<String>::new(1, id, 12);
    let owner = owner.take(wrong).unwrap_err();
    let key = RecoveryKey::<String>::new(1, id, 13);
    assert_eq!(owner.take(key).unwrap(), "owned");
}

#[test]
fn test_recovery_key_matches_erased_owner_type() {
    let id = ResourceId::new(OperationId::new(2), 1, 3);
    let owner = RecoveryOwner::new(&RecoveryKey::<String>::new(1, id, 21), String::from("owned"));
    let wrong_type = RecoveryKey::<u64>::new(1, id, 21);

    let owner = owner.take(wrong_type).unwrap_err();
    assert_eq!(owner.take(RecoveryKey::<String>::new(1, id, 21)).unwrap(), "owned");
}

#[test]
fn test_recovery_key_matches_registry_namespace() {
    let id = ResourceId::new(OperationId::new(3), 2, 6);
    let owner = RecoveryOwner::new(
        &RecoveryKey::<String>::new(17, id, 29),
        String::from("namespace protected"),
    );
    let wrong_registry = RecoveryKey::<String>::new(18, id, 29);
    let owner = owner.take(wrong_registry).unwrap_err();
    assert_eq!(
        owner.take(RecoveryKey::<String>::new(17, id, 29)).unwrap(),
        "namespace protected"
    );
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::report::OperationId;
use crate::report::RecoveryKey;
use crate::report::RecoveryOwner;
use crate::report::ResourceId;

#[test]
fn test_wrong_generation_or_type_key_does_not_consume_recovery_owner() {
    let id = ResourceId::new(OperationId::new(2), 1, 8);
    let key = RecoveryKey::<String>::new(9, id, 44);
    let owner = RecoveryOwner::new(&key, String::from("still owned"));
    let old_generation = RecoveryKey::<String>::new(9, ResourceId::new(OperationId::new(2), 1, 7), 44);
    let owner = owner.take(old_generation).unwrap_err();
    let wrong_type = RecoveryKey::<u64>::new(9, id, 44);
    let owner = owner.take(wrong_type).unwrap_err();
    assert_eq!(owner.take(key).unwrap(), "still owned");
}

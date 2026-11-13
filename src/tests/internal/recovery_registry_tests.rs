// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::future::ready;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::protocol::CleanupContext;
use crate::protocol::OwnedCleanup;
use crate::report::CleanupOutcome;
use crate::report::OperationId;
use crate::report::RecoveryKey;
use crate::report::RecoveryOwner;
use crate::report::ResourceId;
use crate::resource::DependencyError;
use crate::resource::RecoveryRegistry;
use crate::resource::RegisterErrorKind;
use crate::resource::ResourceError;
use crate::resource::ResourceRegistry;

struct CountedCleanup(Arc<AtomicUsize>);

impl OwnedCleanup for CountedCleanup {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(ready(CleanupOutcome::Cleaned))
    }
}

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

#[test]
fn test_recovery_registry_preserves_values_when_capacity_is_full() {
    let id = ResourceId::new(OperationId::new(3), 1, 4);
    let registry = RecoveryRegistry::new(1);
    let key = registry.register(id, String::from("retained")).unwrap();

    assert_eq!(registry.capacity(), 1);
    assert_eq!(registry.len(), 1);
    assert!(registry.keep(&key));
    assert!(matches!(registry.register(id, String::from("rejected")), Err(value) if value == "rejected"));
    assert_eq!(registry.take_recovery(key).unwrap(), "retained");
    assert!(registry.is_empty());
}

#[test]
fn test_recovery_registry_returns_capsule_and_failure_owner_when_full() {
    let id = ResourceId::new(OperationId::new(4), 2, 7);
    let registry = RecoveryRegistry::new(0);

    let rejected = registry.register_with_capsule(id, String::from("capsule"), String::from("failure"));

    assert!(matches!(rejected, Err((capsule, failure)) if capsule == "capsule" && failure == "failure"));
    assert!(registry.is_empty());
}

#[test]
fn test_recovery_registry_clone_shares_take_once_ownership() {
    let id = ResourceId::new(OperationId::new(5), 0, 1);
    let registry = RecoveryRegistry::new(1);
    let clone = registry.clone();
    let key = registry.register(id, 42_u32).unwrap();

    assert_eq!(clone.take_recovery(key).unwrap(), 42);
    assert!(registry.is_empty());
}

#[test]
fn test_recovery_key_mismatches_preserve_recovery_owner() {
    let resource = ResourceId::new(OperationId::new(6), 3, 8);
    let key = RecoveryKey::<String>::new(12, resource, 5);
    let owner = RecoveryOwner::new(&key, String::from("preserved"));
    let wrong_registry = RecoveryKey::<String>::new(13, resource, 5);
    let owner = owner.take(wrong_registry).unwrap_err();
    let wrong_nonce = RecoveryKey::<String>::new(12, resource, 6);
    let owner = owner.take(wrong_nonce).unwrap_err();

    assert_eq!(owner.resource_id(), resource);
    assert_eq!(owner.take(key).unwrap(), "preserved");
}

#[test]
fn test_recovery_registry_retries_only_while_key_is_retained() {
    let resource = ResourceId::new(OperationId::new(7), 0, 2);
    let registry = RecoveryRegistry::new(1);
    let key = registry.register(resource, String::from("capsule")).unwrap();
    let begins = Arc::new(AtomicUsize::new(0));
    let future = match registry.retry_cleanup(
        &key,
        Box::new(CountedCleanup(Arc::clone(&begins))),
        CleanupContext { deadline: None },
    ) {
        Ok(future) => future,
        Err(_) => panic!("current recovery key should start cleanup"),
    };
    assert_eq!(begins.load(Ordering::SeqCst), 1);
    assert!(registry.keep(&key));
    drop(future);

    assert_eq!(registry.take_recovery(key).unwrap(), "capsule");
    let foreign_registry = RecoveryRegistry::new(1);
    let stale = foreign_registry.register(resource, String::from("foreign")).unwrap();
    assert!(
        registry
            .retry_cleanup(
                &stale,
                Box::new(CountedCleanup(Arc::clone(&begins))),
                CleanupContext { deadline: None },
            )
            .is_err()
    );
    assert_eq!(begins.load(Ordering::SeqCst), 1);
}

#[test]
fn test_recovery_registry_waker_registration_tracks_retained_owners() {
    let registry = RecoveryRegistry::new(1);
    let waker = std::task::Waker::noop();
    let resource = ResourceId::new(OperationId::new(8), 1, 0);

    assert!(!registry.register_waker_if_nonempty(waker));
    let key = registry.register(resource, String::from("owner")).unwrap();
    assert!(registry.register_waker_if_nonempty(waker));
    assert_eq!(registry.take_recovery(key).unwrap(), "owner");
    assert!(!registry.register_waker_if_nonempty(waker));
}

#[test]
fn test_register_error_exposes_kind_and_preserves_rejected_owner() {
    let registry = ResourceRegistry::new(OperationId::new(9), 0);
    let begins = Arc::new(AtomicUsize::new(0));
    let error = registry
        .register(Box::new(CountedCleanup(Arc::clone(&begins))))
        .unwrap_err();

    assert_eq!(error.kind(), RegisterErrorKind::CapacityExceeded);
    let _future = error.into_owner().begin(CleanupContext { deadline: None });
    assert_eq!(begins.load(Ordering::SeqCst), 1);
}

#[test]
fn test_resource_handle_closes_future_uses_and_rejects_self_dependency() {
    let registry = ResourceRegistry::new(OperationId::new(10), 1);
    let handle = registry
        .register(Box::new(CountedCleanup(Arc::new(AtomicUsize::new(0)))))
        .unwrap();

    let lease = handle.acquire_use().unwrap();
    assert_eq!(handle.cleanup_after(&handle), Err(DependencyError::SelfDependency));
    handle.close_for_use().unwrap();
    handle.close_for_use().unwrap();
    assert!(matches!(handle.acquire_use(), Err(ResourceError::ClosedForUse)));
    drop(lease);
    let _cleanup = registry.take_ready_cleanup().unwrap();
    assert!(matches!(handle.close_for_use(), Err(ResourceError::AlreadyCleaning)));
}

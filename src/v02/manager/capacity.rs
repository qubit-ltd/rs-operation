// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Bounded capacity and permit accounting.
#![allow(dead_code)] // T04 staging interfaces are consumed by later v0.2 wiring tasks.

mod admission_coordinator;
mod admission_request;
mod capacity_error;
mod capacity_permit;
mod capacity_pool;
mod internal;
mod manager_capacity;
mod manager_capacity_pools;
mod permit_bundle;
mod resource_label;
mod resource_registration;
mod scope_capacity;
mod scope_capacity_kind;

pub(crate) use admission_coordinator::AdmissionCoordinator;
pub(crate) use admission_request::AdmissionRequest;
pub(crate) use capacity_error::CapacityError;
pub(crate) use capacity_permit::CapacityPermit;
pub(crate) use capacity_pool::CapacityPool;
pub(crate) use manager_capacity::ManagerCapacity;
pub(crate) use manager_capacity_pools::ManagerCapacityPools;
pub(crate) use permit_bundle::PermitBundle;
pub(crate) use resource_label::ResourceLabel;
pub(crate) use resource_registration::ResourceRegistration;
pub(crate) use scope_capacity::ScopeCapacity;
pub(crate) use scope_capacity_kind::ScopeCapacityKind;

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    use super::AdmissionRequest;
    use super::CapacityPool;
    use super::ManagerCapacityPools;
    use super::PermitBundle;
    use super::ResourceRegistration;
    use super::ScopeCapacity;
    use crate::v02::report::ManagerLimits;
    use crate::v02::report::ScopeLimits;

    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn poll<F: Future + Unpin>(future: &mut F, waker: &Waker) -> Poll<F::Output> {
        Pin::new(future).poll(&mut Context::from_waker(waker))
    }

    #[test]
    fn test_pool_clone_does_not_duplicate_capacity() {
        let pool = CapacityPool::new("test", 1);
        let clone = pool.clone();
        let permit = pool.try_acquire().expect("first pool handle acquires");

        assert!(clone.try_acquire().is_err());
        drop(permit);
        assert_eq!(clone.available(), 1);
    }

    #[test]
    fn test_failed_bundle_rolls_back_all_pools() {
        let first = CapacityPool::new("first", 1);
        let second = CapacityPool::new("second", 1);
        let held = second.try_acquire().expect("occupy second pool");

        assert!(PermitBundle::try_acquire(&[first.clone(), second.clone()]).is_err());
        assert_eq!(first.available(), 1);
        assert_eq!(second.available(), 0);

        drop(held);
    }

    #[test]
    fn test_root_permits_can_follow_outcome_and_settlement_separately() {
        let manager = ManagerCapacityPools::new(ManagerLimits {
            active_operations: 1,
            unconsumed_outcomes: 1,
            ..ManagerLimits::default()
        });
        let mut bundle = manager
            .root_admission_permits()
            .expect("root and outcome capacities are available");
        let active = bundle
            .take(&manager.active_roots)
            .expect("root permit is separately transferable");
        let outcome = bundle
            .take(&manager.unconsumed_outcomes)
            .expect("outcome permit is separately transferable");

        drop(active);
        assert_eq!(manager.active_roots.available(), 1);
        assert_eq!(manager.unconsumed_outcomes.available(), 0);
        drop(outcome);
        assert_eq!(manager.unconsumed_outcomes.available(), 1);
    }

    #[test]
    fn test_grandchild_uses_root_and_local_pools() {
        let manager = ManagerCapacityPools::new(ManagerLimits::default());
        let root = ScopeCapacity::root(
            &manager,
            1,
            ScopeLimits {
                child_operations: 2,
                resources: 3,
                resource_scopes: 5,
                effect_records: 4,
            },
        );
        let child = root.child(
            2,
            ScopeLimits {
                child_operations: 1,
                resources: 8,
                resource_scopes: 9,
                effect_records: 2,
            },
        );
        let grandchild = child.child(3, ScopeLimits::default());

        assert_eq!(root.effective_limits().child_operations, 2);
        assert_eq!(child.effective_limits().resources, 3);
        assert_eq!(grandchild.effective_limits().effect_records, 2);
        assert_eq!(root.child_capacity_available(), 2);
        let first = grandchild.child_operation_permits().expect("first grandchild");
        assert_eq!(root.child_capacity_available(), 1);
        assert_eq!(child.child_capacity_available(), 0);
        assert!(grandchild.child_operation_permits().is_err());
        drop(first);
        assert_eq!(root.child_capacity_available(), 2);
        assert_eq!(child.child_capacity_available(), 1);
    }

    #[test]
    fn test_resource_scope_capacity_is_shared_with_ancestors_and_reusable() {
        let manager = ManagerCapacityPools::new(ManagerLimits::default());
        let root = ScopeCapacity::root(
            &manager,
            1,
            ScopeLimits {
                resource_scopes: 2,
                ..ScopeLimits::default()
            },
        );
        let child = root.child(
            2,
            ScopeLimits {
                resource_scopes: 5,
                ..ScopeLimits::default()
            },
        );

        let first = child.resource_scope_permits().expect("first scope permit");
        let second = child.resource_scope_permits().expect("second scope permit");
        assert!(child.resource_scope_permits().is_err());
        drop(first);
        let third = child.resource_scope_permits().expect("returned scope permit");
        drop((second, third));
        assert!(child.resource_scope_permits().is_ok());
    }

    #[test]
    fn test_resource_ref_clone_is_not_registration() {
        let manager = ManagerCapacityPools::new(ManagerLimits::default());
        let scope = ScopeCapacity::root(&manager, 10, ScopeLimits::default());
        let registration: ResourceRegistration = scope.resource_registration().expect("resource registration succeeds");
        let clone = registration.clone();
        assert_eq!(clone.permit_count(), 1);

        assert_eq!(
            scope.resource_capacity_available(),
            ScopeLimits::default().resources - 1
        );
        drop(registration);
        assert_eq!(
            scope.resource_capacity_available(),
            ScopeLimits::default().resources - 1
        );
        drop(clone);
        assert_eq!(scope.resource_capacity_available(), ScopeLimits::default().resources);
    }

    #[test]
    fn test_release_notifies_registered_waiter() {
        let pool = CapacityPool::new("one", 1);
        let held = pool.try_acquire().expect("hold the only slot");
        let mut waiting = AdmissionRequest::new(vec![pool.clone()], None).expect("waiter registration has capacity");
        let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));

        assert!(poll(&mut waiting, &waker).is_pending());
        drop(held);
        assert!(wakes.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(poll(&mut waiting, &waker), Poll::Ready(Ok(_))));
    }

    #[test]
    fn test_cancelled_admission_returns_every_permit() {
        let first = CapacityPool::new("first", 1);
        let second = CapacityPool::new("second", 1);
        let waiters = CapacityPool::new("waiter", 1);
        let held_second = second.try_acquire().expect("hold second pool");
        let mut waiting = AdmissionRequest::new(vec![first.clone(), second.clone()], Some(waiters.clone()))
            .expect("waiter slot is available");
        let waker = Waker::from(Arc::new(WakeCount(AtomicUsize::new(0))));

        assert!(poll(&mut waiting, &waker).is_pending());
        assert_eq!(first.available(), 1);
        assert_eq!(waiters.available(), 0);
        drop(waiting);

        assert_eq!(first.available(), 1);
        assert_eq!(second.available(), 0);
        assert_eq!(waiters.available(), 1);
        drop(held_second);
    }

    #[test]
    fn test_root_execution_and_outcome_permits_have_distinct_lifetimes() {
        let manager = ManagerCapacityPools::new(ManagerLimits {
            active_operations: 1,
            unconsumed_outcomes: 1,
            ..ManagerLimits::default()
        });
        let (execution, outcome) = manager.root_lifecycle_permits().unwrap();
        drop(execution);
        let next_execution = manager.active_root_permit().unwrap();
        assert!(manager.outcome_permit().is_err());
        drop(outcome);
        assert!(manager.outcome_permit().is_ok());
        drop(next_execution);
    }
}

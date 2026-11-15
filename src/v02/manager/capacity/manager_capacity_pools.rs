// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::AdmissionCoordinator;
use super::AdmissionRequest;
use super::CapacityError;
use super::CapacityPermit;
use super::CapacityPool;
use super::ManagerCapacity;
use super::PermitBundle;
use super::ResourceLabel;
use super::ScopeCapacity;
use crate::v02::report::ManagerLimits;

/// Manager-wide bounded capacities. Each field is one shared rs-budget pool.
pub(crate) struct ManagerCapacityPools {
    /// Concurrent root execution permits.
    pub(in crate::v02::manager::capacity) active_roots: CapacityPool,
    /// Queued root operation permits.
    pub(in crate::v02::manager::capacity) queued_roots: CapacityPool,
    /// Bounds callers waiting for manager capacity.
    pub(in crate::v02::manager::capacity) admission_waiters: CapacityPool,
    /// Bounds outcomes awaiting consumption.
    pub(in crate::v02::manager::capacity) unconsumed_outcomes: CapacityPool,
    /// Bounds concurrent resource cleanup work.
    pub(in crate::v02::manager::capacity) cleanup_concurrency: CapacityPool,
    /// Bounds retained diagnostic records.
    pub(in crate::v02::manager::capacity) diagnostic_events: CapacityPool,
    /// Wakes waiters when any manager pool returns capacity.
    pub(in crate::v02::manager::capacity) coordinator: Arc<AdmissionCoordinator>,
}

impl ManagerCapacityPools {
    /// Builds manager pools from limits and shares one coordinator across them.
    ///
    /// # Parameters
    ///
    /// - `limits`: Sets the capacity of each manager pool.
    ///
    /// # Returns
    ///
    /// The manager's bounded capacity pools.
    pub(crate) fn new(limits: ManagerLimits) -> Self {
        let coordinator = AdmissionCoordinator::new(limits.admission_waiters);
        let pool = |capacity, label| {
            CapacityPool::with_coordinator(ResourceLabel::Manager(label), capacity, Arc::clone(&coordinator))
        };
        Self {
            active_roots: pool(limits.active_operations, ManagerCapacity::ActiveRoots),
            queued_roots: pool(limits.queued_operations, ManagerCapacity::QueuedRoots),
            admission_waiters: pool(limits.admission_waiters, ManagerCapacity::AdmissionWaiters),
            unconsumed_outcomes: pool(limits.unconsumed_outcomes, ManagerCapacity::UnconsumedOutcomes),
            cleanup_concurrency: pool(limits.cleanup_concurrency, ManagerCapacity::CleanupConcurrency),
            diagnostic_events: pool(limits.diagnostic_events, ManagerCapacity::DiagnosticEvents),
            coordinator,
        }
    }

    /// Acquires the root execution and unconsumed-outcome permits together.
    ///
    /// # Errors
    ///
    /// Returns an error if either required pool has no available capacity.
    pub(crate) fn root_admission_permits(&self) -> Result<PermitBundle, CapacityError> {
        PermitBundle::try_acquire(&[self.active_roots.clone(), self.unconsumed_outcomes.clone()])
    }

    /// Reserves root execution and outcome capacity atomically, then returns
    /// each permit separately so its lifecycle owner can release it at the
    /// corresponding operation fact.
    ///
    /// # Errors
    ///
    /// Returns an error if either required pool has no available capacity.
    pub(crate) fn root_lifecycle_permits(&self) -> Result<(CapacityPermit, CapacityPermit), CapacityError> {
        let mut permits = PermitBundle::try_acquire(&[self.active_roots.clone(), self.unconsumed_outcomes.clone()])?;
        Ok((
            permits.take(&self.active_roots).expect("active root permit"),
            permits.take(&self.unconsumed_outcomes).expect("outcome permit"),
        ))
    }

    /// Acquires one permit for an outcome that remains unconsumed.
    ///
    /// # Errors
    ///
    /// Returns an error if the unconsumed-outcome pool has no available
    /// capacity.
    pub(crate) fn outcome_permit(&self) -> Result<CapacityPermit, CapacityError> {
        self.unconsumed_outcomes.try_acquire()
    }

    /// Acquires a root permit for focused manager-capacity tests.
    ///
    /// # Errors
    ///
    /// Returns an error if the active-root pool has no available capacity.
    #[cfg(test)]
    pub(in crate::v02::manager::capacity) fn active_root_permit(&self) -> Result<CapacityPermit, CapacityError> {
        self.active_roots.try_acquire()
    }

    /// Returns the capacity pools required before admitting a root operation.
    pub(crate) fn root_pools(&self) -> Vec<CapacityPool> {
        vec![self.active_roots.clone(), self.unconsumed_outcomes.clone()]
    }

    /// Creates a waiter that retries root admission when capacity changes.
    ///
    /// # Errors
    ///
    /// Returns an error if a required pool or the admission-waiter pool has no
    /// available capacity.
    pub(crate) fn root_admission_request(&self) -> Result<AdmissionRequest, CapacityError> {
        self.admission_request(vec![self.active_roots.clone(), self.unconsumed_outcomes.clone()])
    }

    /// Creates a waiter for the supplied set of pools.
    ///
    /// # Parameters
    ///
    /// - `pools`: The pools that must all have capacity before admission.
    ///
    /// # Errors
    ///
    /// Returns an error if a requested pool or the admission-waiter pool has no
    /// available capacity.
    pub(crate) fn admission_request(&self, pools: Vec<CapacityPool>) -> Result<AdmissionRequest, CapacityError> {
        AdmissionRequest::new(pools, Some(self.admission_waiters.clone()))
    }

    /// Creates a child waiter covering hierarchical child and outcome pools.
    ///
    /// # Parameters
    ///
    /// - `scope`: Supplies the child's hierarchical operation pools.
    ///
    /// # Errors
    ///
    /// Returns an error if a required pool or the admission-waiter pool has no
    /// available capacity.
    pub(crate) fn child_admission_request(&self, scope: &ScopeCapacity) -> Result<AdmissionRequest, CapacityError> {
        let mut pools = scope.child_operation_pools.clone();
        pools.push(self.unconsumed_outcomes.clone());
        self.admission_request(pools)
    }

    /// Atomically reserves a child's hierarchical lifetime and outcome
    /// capacity.
    ///
    /// # Parameters
    ///
    /// - `scope`: Supplies the child's hierarchical operation pools.
    ///
    /// # Errors
    ///
    /// Returns an error if a required pool has no available capacity.
    pub(crate) fn child_lifecycle_permits(
        &self,
        scope: &ScopeCapacity,
    ) -> Result<(PermitBundle, CapacityPermit), CapacityError> {
        let mut permits = PermitBundle::try_acquire(
            &[
                scope.child_operation_pools.as_slice(),
                std::slice::from_ref(&self.unconsumed_outcomes),
            ]
            .concat(),
        )?;
        let outcome = permits.take(&self.unconsumed_outcomes).expect("outcome permit");
        Ok((permits, outcome))
    }

    /// Returns the distinct pools needed for child operation admission.
    ///
    /// # Parameters
    ///
    /// - `scope`: Supplies the child's hierarchical operation pools.
    pub(crate) fn child_admission_pools(&self, scope: &ScopeCapacity) -> Vec<CapacityPool> {
        let mut pools = scope.child_operation_pools.clone();
        pools.push(self.unconsumed_outcomes.clone());
        pools
    }

    /// Acquires one executor-queue slot.
    ///
    /// # Errors
    ///
    /// Returns an error if the queued-root pool has no available capacity.
    pub(crate) fn queue_permit(&self) -> Result<CapacityPermit, CapacityError> {
        self.queued_roots.try_acquire()
    }

    /// Acquires one concurrent cleanup slot.
    ///
    /// # Errors
    ///
    /// Returns an error if the cleanup-concurrency pool has no available
    /// capacity.
    pub(crate) fn cleanup_permit(&self) -> Result<CapacityPermit, CapacityError> {
        self.cleanup_concurrency.try_acquire()
    }

    /// Acquires one retained diagnostic-event slot.
    ///
    /// # Errors
    ///
    /// Returns an error if the diagnostic-event pool has no available capacity.
    pub(crate) fn diagnostic_permit(&self) -> Result<CapacityPermit, CapacityError> {
        self.diagnostic_events.try_acquire()
    }

    /// Clones the coordinator shared by this manager's capacity pools.
    pub(crate) fn coordinator(&self) -> Arc<AdmissionCoordinator> {
        Arc::clone(&self.coordinator)
    }

    /// Wakes tasks waiting on any pool coordinated by this manager.
    pub(crate) fn notify_waiters(&self) {
        self.coordinator.wake_waiters();
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use super::ManagerCapacityPools;
    use super::ScopeCapacity;
    use crate::v02::report::ManagerLimits;
    use crate::v02::report::ScopeLimits;

    fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn test_manager_pools_cover_root_child_and_auxiliary_lifetimes() {
        let manager = ManagerCapacityPools::new(ManagerLimits {
            active_operations: 1,
            queued_operations: 1,
            admission_waiters: 1,
            unconsumed_outcomes: 1,
            cleanup_concurrency: 1,
            diagnostic_events: 1,
        });

        let root_pools = manager.root_pools();
        assert_eq!(root_pools.len(), 2);
        let mut root_request = Box::pin(manager.root_admission_request().unwrap());
        let root_bundle = match poll(root_request.as_mut()) {
            Poll::Ready(Ok(bundle)) => bundle,
            _ => panic!("available root capacity should admit immediately"),
        };
        assert_eq!(root_bundle.len(), 2);
        drop(root_bundle);

        let scope = ScopeCapacity::root(&manager, 31, ScopeLimits::default());
        assert_eq!(
            manager.child_admission_pools(&scope).len(),
            scope.child_operation_pools.len() + 1
        );
        let mut child_request = Box::pin(manager.child_admission_request(&scope).unwrap());
        assert!(matches!(poll(child_request.as_mut()), Poll::Ready(Ok(_))));
        let (child_bundle, outcome) = manager.child_lifecycle_permits(&scope).unwrap();
        assert!(!child_bundle.is_empty());
        drop((child_bundle, outcome));

        let first_coordinator = manager.coordinator();
        let second_coordinator = manager.coordinator();
        assert!(Arc::ptr_eq(&first_coordinator, &second_coordinator));
        assert!(manager.queue_permit().is_ok());
        assert!(manager.cleanup_permit().is_ok());
        assert!(manager.diagnostic_permit().is_ok());
        assert!(manager.outcome_permit().is_ok());
        manager.notify_waiters();
    }
}

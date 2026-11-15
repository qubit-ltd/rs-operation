// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;

use super::AdmissionCoordinator;
use super::CapacityError;
use super::CapacityPermit;
use super::CapacityPool;
use super::PermitBundle;
use super::ResourceLabel;
use super::internal::NEXT_WAITER_ID;
use super::internal::default_coordinator;

/// A cancellable admission future. It never holds a partial bundle while
/// pending; only the bounded waiter registration remains reserved.
#[must_use]
pub(crate) struct AdmissionRequest {
    /// Pools that must be acquired together before the future completes.
    pools: Vec<CapacityPool>,
    /// Coordinator registering the current task waker.
    coordinator: Arc<AdmissionCoordinator>,
    /// Stable id for replacing/unregistering this future's waiter.
    waiter_id: u64,
    /// Bounded permit retained only while waiting for pool capacity.
    waiter_permit: Option<CapacityPermit>,
    /// Whether this future currently owns a waiter registration slot.
    registered: bool,
}

impl AdmissionRequest {
    /// Prepares an atomic capacity request and reserves a bounded waiter slot.
    ///
    /// Returns an error when the waiter capacity is exhausted or the pools use
    /// different coordinators.
    ///
    /// # Parameters
    ///
    /// - `pools`: capacity pools that must be acquired atomically.
    /// - `waiter_pool`: optional bounded pool used to reserve a waiter slot.
    ///
    /// # Returns
    ///
    /// A request future that resolves to the whole permit bundle or a capacity
    /// error when polled.
    ///
    /// # Errors
    ///
    /// Returns `CapacityError` when the waiter slot is unavailable or the
    /// pools do not share one coordinator.
    pub(crate) fn new(pools: Vec<CapacityPool>, waiter_pool: Option<CapacityPool>) -> Result<Self, CapacityError> {
        let mut pools = pools;
        pools.sort_unstable_by_key(CapacityPool::id);
        pools.dedup_by_key(|pool| pool.id());
        let coordinator = pools
            .first()
            .map(|pool| Arc::clone(&pool.inner.coordinator))
            .or_else(|| waiter_pool.as_ref().map(|pool| Arc::clone(&pool.inner.coordinator)))
            .unwrap_or_else(default_coordinator);
        if pools
            .iter()
            .any(|pool| !Arc::ptr_eq(&coordinator, &pool.inner.coordinator))
            || waiter_pool
                .as_ref()
                .is_some_and(|pool| !Arc::ptr_eq(&coordinator, &pool.inner.coordinator))
        {
            let resource = waiter_pool
                .as_ref()
                .map(|pool| pool.inner.budget.resource().clone())
                .or_else(|| pools.first().map(|pool| pool.inner.budget.resource().clone()))
                .unwrap_or(ResourceLabel::Named("capacity coordinator"));
            return Err(CapacityError { resource });
        }
        let waiter_permit = waiter_pool.map(|pool| pool.try_acquire()).transpose()?;
        Ok(Self {
            pools,
            coordinator,
            waiter_id: NEXT_WAITER_ID.fetch_add(1, Ordering::Relaxed),
            waiter_permit,
            registered: false,
        })
    }
}

impl Future for AdmissionRequest {
    /// The complete permit bundle or the capacity error that prevented it.
    type Output = Result<PermitBundle, CapacityError>;

    /// Acquires the whole permit bundle or registers the current task to be
    /// woken.
    ///
    /// # Parameters
    ///
    /// - `self`: pinned admission future being polled.
    /// - `cx`: task context used to register the current waker.
    ///
    /// # Returns
    ///
    /// `Pending` while capacity is unavailable; `Ready(Ok(_))` with the whole
    /// bundle on success or `Ready(Err(_))` when a pool cannot be acquired.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.registered {
            self.coordinator.unregister(self.waiter_id);
            self.registered = false;
        }
        let observed_generation = self.coordinator.generation();
        match PermitBundle::try_acquire(&self.pools) {
            Ok(bundle) => {
                self.coordinator.unregister(self.waiter_id);
                self.registered = false;
                drop(self.waiter_permit.take());
                Poll::Ready(Ok(bundle))
            }
            Err(_) => {
                self.coordinator
                    .register(self.waiter_id, observed_generation, cx.waker());
                self.registered = true;
                match PermitBundle::try_acquire(&self.pools) {
                    Ok(bundle) => {
                        self.coordinator.unregister(self.waiter_id);
                        self.registered = false;
                        drop(self.waiter_permit.take());
                        Poll::Ready(Ok(bundle))
                    }
                    Err(_) => Poll::Pending,
                }
            }
        }
    }
}

impl Drop for AdmissionRequest {
    /// Unregisters a pending waiter and returns its bounded waiter permit.
    fn drop(&mut self) {
        if self.registered {
            self.coordinator.unregister(self.waiter_id);
        }
        drop(self.waiter_permit.take());
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use super::AdmissionRequest;
    use super::CapacityPool;
    use crate::v02::report::ManagerLimits;

    fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn test_admission_request_rejects_mixed_coordinators_and_full_waiter_pool() {
        let first = crate::v02::manager::capacity::ManagerCapacityPools::new(ManagerLimits {
            admission_waiters: 1,
            ..ManagerLimits::default()
        });
        let second = crate::v02::manager::capacity::ManagerCapacityPools::new(ManagerLimits::default());

        assert!(AdmissionRequest::new(vec![first.active_roots.clone(), second.active_roots.clone()], None,).is_err());
        assert!(
            AdmissionRequest::new(vec![first.active_roots.clone()], Some(second.admission_waiters.clone()),).is_err()
        );

        let waiter = first.admission_waiters.try_acquire().unwrap();
        assert!(AdmissionRequest::new(Vec::new(), Some(first.admission_waiters.clone())).is_err());
        drop(waiter);
        assert!(AdmissionRequest::new(Vec::new(), None).is_ok());
    }

    #[test]
    fn test_empty_admission_request_completes_with_empty_bundle() {
        let mut request = Box::pin(AdmissionRequest::new(Vec::new(), None).unwrap());
        match poll(request.as_mut()) {
            Poll::Ready(Ok(bundle)) => assert!(bundle.is_empty()),
            _ => panic!("an empty capacity request should complete immediately"),
        }
    }

    #[test]
    fn test_admission_request_repoll_unregisters_then_acquires_returned_capacity() {
        let pool = CapacityPool::new("repolled admission", 1);
        let held = pool.try_acquire().unwrap();
        let mut request = Box::pin(AdmissionRequest::new(vec![pool.clone()], None).unwrap());
        assert!(poll(request.as_mut()).is_pending());
        drop(held);
        assert!(matches!(poll(request.as_mut()), Poll::Ready(Ok(_))));
    }
}

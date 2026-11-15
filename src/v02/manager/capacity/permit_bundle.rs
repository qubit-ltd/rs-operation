// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::AdmissionCoordinator;
use super::CapacityError;
use super::CapacityPermit;
use super::CapacityPool;

/// A rollback-safe collection of permits acquired in a canonical pool order.
#[must_use]
pub(crate) struct PermitBundle {
    /// Acquired tokens retained until individually transferred or dropped.
    permits: Vec<CapacityPermit>,
    /// Shared waiter coordinator notified after the final bundle release.
    coordinator: Option<Arc<AdmissionCoordinator>>,
}

impl PermitBundle {
    /// Acquires all distinct pools in stable order or rolls back every partial
    /// acquisition.
    ///
    /// # Parameters
    ///
    /// - `pools`: The capacity pools to acquire.
    ///
    /// # Returns
    ///
    /// A bundle containing one permit for each distinct pool.
    ///
    /// # Errors
    ///
    /// Returns an error if a pool is exhausted or the pools do not share a
    /// coordinator.
    pub(crate) fn try_acquire(pools: &[CapacityPool]) -> Result<Self, CapacityError> {
        let mut ordered: Vec<_> = pools.iter().collect();
        ordered.sort_unstable_by_key(|pool| pool.id());
        ordered.dedup_by_key(|pool| pool.id());

        let coordinator = ordered.first().map(|pool| Arc::clone(&pool.inner.coordinator));
        if let Some(coordinator) = &coordinator
            && ordered
                .iter()
                .any(|pool| !Arc::ptr_eq(coordinator, &pool.inner.coordinator))
        {
            return Err(CapacityError {
                resource: ordered[0].inner.budget.resource().clone(),
            });
        }
        let transaction = coordinator.as_ref().map(|coordinator| {
            coordinator
                .transaction
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        let mut permits = Vec::with_capacity(ordered.len());
        for pool in ordered {
            match pool.try_acquire() {
                Ok(mut permit) => {
                    permit.suppress_notification();
                    permits.push(permit);
                }
                Err(error) => {
                    drop(permits);
                    drop(transaction);
                    return Err(error);
                }
            }
        }
        drop(transaction);
        for permit in &mut permits {
            permit.notify_on_drop = true;
        }
        Ok(Self { permits, coordinator })
    }

    /// Returns the number of permits still owned by this bundle.
    ///
    /// # Parameters
    ///
    /// - `self`: The bundle whose retained permit count is returned.
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.permits.len()
    }

    /// Transfers one permit associated with `pool` to a lifecycle-specific
    /// owner.
    ///
    /// # Parameters
    ///
    /// - `self`: The bundle from which the matching permit is transferred.
    /// - `pool`: Selects the pool whose permit should be transferred.
    ///
    /// # Returns
    ///
    /// The matching permit, or `None` if the bundle does not retain one.
    pub(crate) fn take(&mut self, pool: &CapacityPool) -> Option<CapacityPermit> {
        self.permits
            .iter()
            .position(|permit| permit.pool_id == pool.id())
            .map(|index| self.permits.swap_remove(index))
    }

    /// Reports whether this bundle retains any permits.
    ///
    /// # Parameters
    ///
    /// - `self`: The bundle whose retained permits are checked.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.permits.is_empty()
    }
}

impl Drop for PermitBundle {
    /// Releases retained permits together and sends one shared notification.
    ///
    /// # Parameters
    ///
    /// - `self`: The bundle whose remaining permits are released.
    fn drop(&mut self) {
        let had_permits = !self.permits.is_empty();
        for permit in &mut self.permits {
            permit.suppress_notification();
        }
        self.permits.clear();
        if had_permits && let Some(coordinator) = &self.coordinator {
            let transaction = coordinator
                .transaction
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            drop(transaction);
            coordinator.notify();
        }
    }
}

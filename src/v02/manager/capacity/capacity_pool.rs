// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::Ordering;

use qubit_budget::ManagedResourcePool;

use super::AdmissionCoordinator;
use super::CapacityError;
use super::CapacityPermit;
use super::ResourceLabel;
use super::internal::NEXT_POOL_ID;
use super::internal::PoolInner;
use super::internal::default_coordinator;

/// Cloneable pool handle; clones share the single underlying `qubit-budget`
/// pool.
#[must_use]
#[derive(Clone)]
pub(crate) struct CapacityPool {
    /// Shared pool identity, budget, and admission coordinator.
    pub(in crate::v02::manager::capacity) inner: Arc<PoolInner>,
}

impl CapacityPool {
    /// Creates a finite pool using the process-wide waiter coordinator.
    ///
    /// # Parameters
    ///
    /// - `label`: Identifies the managed resource.
    /// - `limit`: Sets the pool's capacity.
    ///
    /// # Returns
    ///
    /// The newly created capacity pool.
    pub(crate) fn new(label: impl Into<ResourceLabel>, limit: usize) -> Self {
        Self::with_coordinator(label, limit, default_coordinator())
    }

    /// Creates a pool that participates in the supplied coordinator.
    ///
    /// # Parameters
    ///
    /// - `label`: Identifies the managed resource.
    /// - `limit`: Sets the pool's capacity.
    /// - `coordinator`: Coordinates admission waiters for related pools.
    ///
    /// # Returns
    ///
    /// The newly created capacity pool.
    pub(in crate::v02::manager::capacity) fn with_coordinator(
        label: impl Into<ResourceLabel>,
        limit: usize,
        coordinator: Arc<AdmissionCoordinator>,
    ) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
                budget: ManagedResourcePool::new(label.into(), limit),
                coordinator,
            }),
        }
    }

    /// Acquires one unit or returns the label of the exhausted pool.
    ///
    /// # Returns
    ///
    /// A permit for one unit from this pool.
    ///
    /// # Errors
    ///
    /// Returns [`CapacityError`] when the pool has no available unit.
    pub(crate) fn try_acquire(&self) -> Result<CapacityPermit, CapacityError> {
        self.inner
            .budget
            .try_acquire(1)
            .map(|permit| CapacityPermit {
                permit: Some(permit),
                coordinator: Arc::clone(&self.inner.coordinator),
                pool_id: self.inner.id,
                notify_on_drop: true,
            })
            .map_err(|_| CapacityError {
                resource: self.inner.budget.resource().clone(),
            })
    }

    /// Returns the number of currently available units.
    ///
    /// # Returns
    ///
    /// The number of units that can be acquired immediately.
    #[must_use]
    #[inline]
    pub(crate) fn available(&self) -> usize {
        self.inner.budget.available()
    }

    /// Returns the stable id shared by all clones of this pool.
    ///
    /// # Returns
    ///
    /// The pool's stable identity.
    #[must_use]
    #[inline]
    pub(in crate::v02::manager::capacity) fn id(&self) -> u64 {
        self.inner.id
    }
}

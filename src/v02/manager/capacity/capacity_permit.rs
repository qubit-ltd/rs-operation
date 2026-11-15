// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use qubit_budget::ManagedResourcePermit;

use super::AdmissionCoordinator;
use super::ResourceLabel;

/// One capacity unit returned to the pool and coordinator on drop.
#[must_use]
pub(crate) struct CapacityPermit {
    /// Underlying budget token released when this wrapper is dropped.
    pub(in crate::v02::manager::capacity) permit: Option<ManagedResourcePermit<ResourceLabel, usize>>,
    /// Wakes registered admission requests after the token is returned.
    pub(in crate::v02::manager::capacity) coordinator: Arc<AdmissionCoordinator>,
    /// Pool identity used to transfer a specific token from a bundle.
    pub(in crate::v02::manager::capacity) pool_id: u64,
    /// Suppresses individual notifications during atomic bundle rollback.
    pub(in crate::v02::manager::capacity) notify_on_drop: bool,
}

impl CapacityPermit {
    /// Defers wakeups while a multi-pool transaction is still rolling back.
    #[inline]
    pub(in crate::v02::manager::capacity) fn suppress_notification(&mut self) {
        self.notify_on_drop = false;
    }
}

impl Drop for CapacityPermit {
    /// Returns the token before notifying admission waiters.
    fn drop(&mut self) {
        drop(self.permit.take());
        if self.notify_on_drop {
            let transaction = self
                .coordinator
                .transaction
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            drop(transaction);
            self.coordinator.notify();
        }
    }
}

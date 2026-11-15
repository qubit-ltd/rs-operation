// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use qubit_budget::ManagedResourcePool;

use super::super::AdmissionCoordinator;
use super::super::ResourceLabel;

/// Shared identity, budget, and waiter coordinator for capacity pool handles.
pub(in crate::v02::manager::capacity) struct PoolInner {
    /// Stable identity used to order pool acquisition and find permits.
    pub(in crate::v02::manager::capacity) id: u64,
    /// Underlying finite `qubit-budget` resource pool.
    pub(in crate::v02::manager::capacity) budget: ManagedResourcePool<ResourceLabel, usize>,
    /// Coordinator notified when this pool's permits are returned.
    pub(in crate::v02::manager::capacity) coordinator: Arc<AdmissionCoordinator>,
}

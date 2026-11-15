// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod coordinator_state;
mod pool_inner;
mod waiter;

use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;

pub(super) use coordinator_state::CoordinatorState;
pub(super) use pool_inner::PoolInner;
pub(super) use waiter::Waiter;

use super::AdmissionCoordinator;

pub(super) static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);
pub(super) static NEXT_WAITER_ID: AtomicU64 = AtomicU64::new(1);

/// Returns the process-wide coordinator used by capacity pools without an
/// explicit coordinator.
///
/// # Returns
///
/// A shared coordinator with a bounded waiter registration capacity.
pub(super) fn default_coordinator() -> Arc<AdmissionCoordinator> {
    static COORDINATOR: OnceLock<Arc<AdmissionCoordinator>> = OnceLock::new();
    Arc::clone(COORDINATOR.get_or_init(|| AdmissionCoordinator::new(4096)))
}

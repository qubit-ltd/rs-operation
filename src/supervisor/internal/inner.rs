// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared supervisor capacities, driver, and waiter registries.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

use super::SharedServiceGroupState;
use super::State;
use crate::driver::PortableDriver;
use crate::report::SupervisorLimits;
use crate::resource::CleanupResponsibility;
use crate::resource::CleanupScheduler;
use crate::resource::RecoveryRegistry;

/// Shared mutable state retained by public and internal supervisor handles.
pub(in crate::supervisor) struct Inner {
    /// Immutable root, waiter, result, and cleanup reservation limits.
    pub(in crate::supervisor) limits: SupervisorLimits,
    /// Counters, queues, and shutdown registrations protected by one mutex.
    pub(in crate::supervisor) state: Mutex<State>,
    /// Optional driver kept alive while managed tasks are running.
    pub(in crate::supervisor) driver: Mutex<Option<Arc<PortableDriver>>>,
    /// Scheduler used to dispatch resource cleanup work.
    pub(in crate::supervisor) cleanup_scheduler: CleanupScheduler,
    /// Cleanup responsibility shared with resource registries.
    pub(in crate::supervisor) cleanup_responsibility: CleanupResponsibility,
    /// Wakes blocking observers when supervisor state changes.
    pub(in crate::supervisor) changed: Condvar,
    /// Registries that can recover resources after operation failure.
    pub(in crate::supervisor) recovery_registries: Mutex<Vec<RecoveryRegistry>>,
    /// Number of externally owned handles controlling automatic shutdown.
    pub(in crate::supervisor) public_handles: AtomicUsize,
    /// Service groups whose admission and cleanup lifecycles remain active.
    pub(in crate::supervisor) service_groups: Mutex<Vec<SharedServiceGroupState>>,
}

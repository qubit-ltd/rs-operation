// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission lease retained for the lifetime of one service request.

use std::sync::Arc;

use super::OperationSupervisor;
use super::internal::SharedServiceGroupState;
use super::internal::service_group_lifecycle::notify_shutdown;
use super::internal::service_group_lifecycle::start_service_resource_cleanup;
use super::internal::util::wake_shutdown;

/// Holds one service-group admission slot until the request finishes.
///
/// Dropping the lease releases the slot and may start cleanup for a group that
/// has already shut down.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::SupervisorLimits;
/// use qubit_operation::supervisor::OperationSupervisor;
///
/// let supervisor = OperationSupervisor::new(SupervisorLimits::default());
/// let group = supervisor.service_group(1).expect("valid capacity");
/// let request = group.try_admit().expect("available request slot");
/// assert_eq!(group.active_requests(), 1);
/// drop(request);
/// assert_eq!(group.active_requests(), 0);
/// ```
pub struct ServiceRequest {
    /// Shared admission state used to release this request's slot.
    pub(super) state: SharedServiceGroupState,
    /// Identifier of this request in the shared admission state.
    pub(super) id: u64,
    /// Supervisor responsible for shutdown notifications and cleanup.
    pub(super) supervisor: OperationSupervisor,
    /// Records whether the request slot has already been released.
    pub(super) released: bool,
}

impl Drop for ServiceRequest {
    /// Releases this request slot and wakes observers waiting for shutdown.
    fn drop(&mut self) {
        if !self.released {
            let (lock, changed) = &*self.state;
            let (waiters, _) = {
                let mut state = lock.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                state.requests.remove(&self.id);
                self.released = true;
                (std::mem::take(&mut state.shutdown_waiters), ())
            };
            changed.notify_all();
            wake_shutdown(waiters);
            notify_shutdown(&self.supervisor);
            start_service_resource_cleanup(Arc::clone(&self.state), self.supervisor.internal_handle());
        }
    }
}

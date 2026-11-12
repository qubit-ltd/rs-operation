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
use super::internal::service_group_lifecycle::{notify_shutdown, start_service_resource_cleanup};

/// Admission lease held for the lifetime of one service request.
pub struct ServiceRequest {
    pub(super) state: SharedServiceGroupState,
    pub(super) id: u64,
    pub(super) supervisor: OperationSupervisor,
    released: bool,
}

impl Drop for ServiceRequest {
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


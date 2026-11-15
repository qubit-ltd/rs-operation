// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::super::cleanup_dispatcher::CleanupDispatcher;
use super::super::internal::CleanupDispatcherInner;
use super::super::lock;
use crate::v02::resource::cleanup_port::CleanupDispatchPermit;
use crate::v02::resource::cleanup_port::CleanupJob;

/// Holds one reserved slot in the cleanup dispatcher's bounded queue.
///
/// Enqueuing consumes the permit and transfers the reservation to a job. If
/// the permit is dropped first, its destructor releases the reservation.
pub(in crate::v02::resource::cleanup) struct DispatchPermit {
    /// Dispatcher state whose reservation this permit owns.
    pub(in crate::v02::resource::cleanup) inner: Arc<CleanupDispatcherInner>,
    /// Whether dropping this permit must release its reserved queue slot.
    pub(in crate::v02::resource::cleanup) active: bool,
}

impl CleanupDispatchPermit for DispatchPermit {
    /// Enqueues a job using the slot reserved by this permit.
    ///
    /// Transfers the job to the dispatcher, then registers and kicks its
    /// background pump when present. The dispatcher also drives pending work
    /// synchronously up to its configured capacity.
    ///
    /// # Parameters
    ///
    /// - `self`: Reserved queue slot being consumed.
    /// - `job`: Cleanup work item admitted to the reserved slot.
    fn enqueue(mut self: Box<Self>, job: Box<dyn CleanupJob>) {
        {
            let mut state = lock(&self.inner.state);
            debug_assert!(self.active);
            debug_assert!(state.pending.len() < self.inner.capacity);
            state.pending.push_back(job);
            self.active = false;
        }
        if let Some(pump) = &self.inner.pump {
            pump.register(&self.inner);
            pump.kick();
        }
        CleanupDispatcher::drive_inner(&self.inner, self.inner.capacity);
    }
}

impl Drop for DispatchPermit {
    /// Releases the queue reservation if it was not transferred to a job.
    fn drop(&mut self) {
        if self.active {
            let mut state = lock(&self.inner.state);
            state.reserved -= 1;
        }
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::cleanup_dispatch_permit::CleanupDispatchPermit;
use super::cleanup_queue_error::CleanupQueueError;

/// Reserves bounded storage for one resource's eventual cleanup handoff.
pub(crate) trait CleanupDispatch: Send + Sync + 'static {
    /// Reserves one bounded queue slot before a cleanup owner is enqueued.
    ///
    /// The returned permit owns the reservation until its job is enqueued or
    /// the permit is dropped, keeping concurrent admissions within capacity.
    ///
    /// # Parameters
    ///
    /// - `self`: Dispatcher whose queue capacity and admission state are
    ///   checked.
    ///
    /// # Returns
    ///
    /// A permit for enqueuing one cleanup job.
    ///
    /// # Errors
    ///
    /// Returns [`CleanupQueueError::Closed`] after admission closes, or
    /// [`CleanupQueueError::Full`] when every reservation slot is in use.
    fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError>;
}

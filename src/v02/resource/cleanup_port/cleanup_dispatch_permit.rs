// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::cleanup_job::CleanupJob;

/// Reservation that guarantees the later enqueue cannot reject or drop its job.
pub(crate) trait CleanupDispatchPermit: Send + 'static {
    /// Transfers a cleanup job into the queue using this reserved slot.
    ///
    /// Consumes the permit so the slot cannot be used to enqueue a second job.
    /// The queue takes ownership of `job` and is responsible for dispatching
    /// it.
    ///
    /// # Parameters
    ///
    /// - `self`: Permit holding the capacity reserved for this handoff.
    /// - `job`: Cleanup work whose ownership moves to the dispatcher.
    fn enqueue(self: Box<Self>, job: Box<dyn CleanupJob>);
}

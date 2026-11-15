// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::super::ScopeCompletion;
use super::scheduled_drop::ScheduledDrop;
use crate::v02::executor::CleanupExecutor;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::report::ResourceId;
use crate::v02::resource::cleanup_port::CleanupJob;

/// Holds a resource until a bounded cleanup executor accepts its destructor.
///
/// The job retains its registration and scope completion state while waiting
/// for a destructor permit. If abandoned before scheduling, it deliberately
/// leaks the resource and registration rather than running the destructor on
/// the dropping thread or claiming successful settlement.
///
/// # Type Parameters
///
/// - `T`: Resource value whose destructor is scheduled on the cleanup executor.
pub(in crate::v02::resource::core) struct DefaultDropJob<T> {
    /// Resource value awaiting execution on a cleanup worker.
    pub(in crate::v02::resource::core) value: Option<T>,
    /// Capacity registration kept until the resource is actually dropped.
    pub(in crate::v02::resource::core) registration: Option<ResourceRegistration>,
    /// Identity reported when the scheduled destructor completes.
    pub(in crate::v02::resource::core) resource_id: ResourceId,
    /// Scope state notified by the scheduled destructor after it runs.
    pub(in crate::v02::resource::core) completion: Arc<ScopeCompletion>,
}
impl<T> Drop for DefaultDropJob<T> {
    /// Prevents an abandoned job from running user destruction on this thread.
    ///
    /// Leaks any still-owned value and registration, leaving settlement
    /// pending because cleanup was not confirmed.
    fn drop(&mut self) {
        // An unrun cleanup job must never destroy its owner on the dropping
        // thread. The reserved dispatcher normally keeps it queued; leaking is
        // the last-resort behavior if that invariant is violated.
        if let Some(value) = self.value.take() {
            std::mem::forget(value);
        }
        if let Some(registration) = self.registration.take() {
            std::mem::forget(registration);
        }
        // Keep the settlement pending when an accepted cleanup job itself is
        // abandoned. No Released fact is synthesized after losing its owner.
    }
}
impl<T: Send + 'static> CleanupJob for DefaultDropJob<T> {
    /// Schedules the resource destructor on the cleanup executor.
    ///
    /// If no destructor permit is available, returns this job intact so its
    /// dispatcher can retry without dropping the resource.
    ///
    /// # Parameters
    ///
    /// - `self`: Job retaining the resource and its scope registration.
    /// - `executor`: Cleanup executor whose bounded destructor queue is used.
    ///
    /// # Returns
    ///
    /// `Ok(())` after ownership is transferred to a scheduled drop; otherwise
    /// returns the unchanged job for retry.
    ///
    /// # Errors
    ///
    /// Returns the job when the executor cannot reserve a destructor slot.
    /// The dispatcher retains it and may retry after capacity becomes
    /// available.
    ///
    /// # Panics
    ///
    /// Panics if the job has already lost its resource or registration, which
    /// violates its ownership invariant.
    fn schedule(mut self: Box<Self>, executor: Arc<CleanupExecutor>) -> Result<(), Box<dyn CleanupJob>> {
        let permit = match executor.reserve_drop() {
            Ok(permit) => permit,
            Err(_) => return Err(self),
        };
        let value = self.value.take().expect("cleanup job owns resource");
        let registration = self.registration.take().expect("cleanup job owns registration");
        let scheduled = ScheduledDrop {
            value: Some(value),
            registration: Some(registration),
            resource_id: self.resource_id,
            completion: Arc::clone(&self.completion),
        };
        permit.drop_value(scheduled);
        Ok(())
    }
}

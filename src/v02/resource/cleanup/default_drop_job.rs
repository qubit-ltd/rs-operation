// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use super::UnconfirmedCleanup;
use crate::v02::executor::CleanupExecutor;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::report::CleanupEvidence;
use crate::v02::resource::cleanup_port::CleanupJob;

/// One-shot callback that receives drop evidence or the panic capsule.
type DropCompletion = Box<dyn FnOnce(Result<CleanupEvidence, UnconfirmedCleanup>) + Send + 'static>;

/// Runs a resource destructor on the cleanup executor and reports its result.
///
/// The job retains its resource when the executor cannot accept it. A panic
/// during destruction is caught and returned as unconfirmed cleanup so the
/// resource registration is not reported as released.
///
/// # Type Parameters
///
/// - `T`: The sendable resource value whose destructor is the cleanup action.
pub(crate) struct DefaultDropJob<T: Send + 'static> {
    /// Resource moved into the executor's drop permit when scheduling succeeds.
    resource: Option<T>,
    /// Capacity registration released only after a confirmed destructor return.
    registration: Option<ResourceRegistration>,
    /// One-shot completion notification for confirmed or unconfirmed cleanup.
    completed: Option<DropCompletion>,
}

impl<T: Send + 'static> DefaultDropJob<T> {
    /// Creates a job that treats dropping `resource` as its cleanup action.
    ///
    /// # Parameters
    ///
    /// - `resource`: Value whose destructor releases the resource.
    /// - `registration`: Optional capacity registration retained until the
    ///   destructor completes or is reported unconfirmed.
    /// - `completed`: One-shot callback that receives drop evidence or the
    ///   panic capsule.
    ///
    /// # Returns
    ///
    /// A pending job that owns the resource, registration and callback.
    pub(crate) fn new(
        resource: T,
        registration: Option<ResourceRegistration>,
        completed: impl FnOnce(Result<CleanupEvidence, UnconfirmedCleanup>) + Send + 'static,
    ) -> Self {
        Self {
            resource: Some(resource),
            registration,
            completed: Some(Box::new(completed)),
        }
    }
}

impl<T: Send + 'static> CleanupJob for DefaultDropJob<T> {
    /// Offers the job to the executor's bounded destructor queue.
    ///
    /// If the executor has no drop permit, returns the unchanged job so its
    /// dispatcher can retain ownership and retry later.
    ///
    /// # Parameters
    ///
    /// - `self`: The pending cleanup job offered to the executor.
    /// - `executor`: Executor that runs resource destructors on its worker.
    ///
    /// # Returns
    ///
    /// `Ok(())` when accepted, or the original job when no permit is available.
    fn schedule(self: Box<Self>, executor: Arc<CleanupExecutor>) -> Result<(), Box<dyn CleanupJob>> {
        match executor.reserve_drop() {
            Ok(permit) => {
                permit.drop_value(self);
                Ok(())
            }
            Err(_) => Err(self),
        }
    }
}

impl<T: Send + 'static> Drop for DefaultDropJob<T> {
    /// Drops an unscheduled resource and reports whether its destructor
    /// returned.
    ///
    /// If destruction panics, keeps the registration inside the unconfirmed
    /// result instead of releasing it as successful cleanup.
    ///
    /// # Parameters
    ///
    /// - `self`: The job whose resource remains locally owned at drop time.
    fn drop(&mut self) {
        let resource = self.resource.take();
        let drop_result = panic::catch_unwind(AssertUnwindSafe(|| drop(resource)));
        if let Some(completed) = self.completed.take() {
            match drop_result {
                Ok(()) => {
                    drop(self.registration.take());
                    completed(Ok(CleanupEvidence::DropCompleted));
                }
                Err(panic) => completed(Err(UnconfirmedCleanup::from_panic(self.registration.take(), panic))),
            }
        }
    }
}

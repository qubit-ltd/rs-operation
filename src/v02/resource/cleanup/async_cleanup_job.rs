// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use super::AsyncCleanup;
use super::AsyncCleanupCompletion;
use super::internal::AsyncCleanupTask;
use crate::v02::context::CleanupContext;
use crate::v02::executor::CleanupExecutor;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::resource::cleanup_port::CleanupJob;

/// Async cleanup job scheduled through the same bounded dispatcher as
/// default drops. Failed results preserve the exact resource and policy.
///
/// # Type Parameters
///
/// - `T`: The thread-safe resource type owned until the cleanup task completes.
pub(crate) struct AsyncCleanupJob<T: Send + Sync + 'static> {
    /// Resource ownership retained until the task is created or the job
    /// retries.
    resource: Option<T>,
    /// Shared cleanup policy kept alive for failure recovery.
    policy: Arc<dyn AsyncCleanup<T>>,
    /// Context passed to the policy when its future is first polled.
    context: Option<CleanupContext>,
    /// Registration transferred to the task and then its completion handler.
    registration: Option<ResourceRegistration>,
    /// Result slot used when no direct callback is configured.
    completion: Arc<Mutex<Option<AsyncCleanupCompletion<T>>>>,
    /// Optional one-shot completion handler, invoked instead of filling the
    /// slot.
    callback: Option<Box<dyn FnOnce(AsyncCleanupCompletion<T>) + Send + 'static>>,
}

impl<T: Send + Sync + 'static> AsyncCleanupJob<T> {
    /// Creates a job that reports completion through a shared result slot.
    ///
    /// The job initially owns the resource and policy. If the executor has no
    /// async capacity, scheduling returns the job unchanged to its dispatcher.
    ///
    /// # Parameters
    ///
    /// - `resource`: The value whose cleanup policy will run.
    /// - `policy`: Shared cleanup policy retained for retry after failure.
    /// - `context`: Scope-provided cleanup cancellation and diagnostic context.
    /// - `completion`: Slot populated when the task completes or becomes
    ///   unconfirmed.
    ///
    /// # Returns
    ///
    /// A pending job that owns the resource, policy, context and result slot.
    pub(crate) fn new(
        resource: T,
        policy: Arc<dyn AsyncCleanup<T>>,
        context: CleanupContext,
        completion: Arc<Mutex<Option<AsyncCleanupCompletion<T>>>>,
    ) -> Self {
        Self {
            resource: Some(resource),
            policy,
            context: Some(context),
            registration: None,
            completion,
            callback: None,
        }
    }

    /// Transfers a resource registration into the cleanup task.
    ///
    /// The registration remains associated with the resource through cleanup
    /// and is handed to the completion handler for release or recovery.
    ///
    /// # Parameters
    ///
    /// - `self`: The pending job whose registration is being attached.
    /// - `registration`: The capacity registration held for this resource.
    ///
    /// # Returns
    ///
    /// The same job with the registration retained for cleanup completion.
    pub(crate) fn with_registration(mut self, registration: ResourceRegistration) -> Self {
        self.registration = Some(registration);
        self
    }

    /// Delivers the completion directly to a callback instead of the result
    /// slot.
    ///
    /// The callback runs after the task has produced a completion record.
    ///
    /// # Parameters
    ///
    /// - `self`: The pending job receiving the completion handler.
    /// - `callback`: A one-shot handler for the completed cleanup result.
    ///
    /// # Returns
    ///
    /// The same job configured to invoke the supplied callback on completion.
    pub(crate) fn with_callback(mut self, callback: impl FnOnce(AsyncCleanupCompletion<T>) + Send + 'static) -> Self {
        self.callback = Some(Box::new(callback));
        self
    }
}

impl<T: Send + Sync + 'static> CleanupJob for AsyncCleanupJob<T> {
    /// Reserves executor capacity and starts polling the cleanup task.
    ///
    /// If no executor permit is available, returns this job so the dispatcher
    /// can keep the resource and policy queued for a later attempt.
    ///
    /// # Parameters
    ///
    /// - `self`: The job whose ownership is transferred to the executor on
    ///   success.
    /// - `executor`: The bounded executor used to drive cleanup.
    ///
    /// # Returns
    ///
    /// `Ok(())` after the task is accepted, or the original job on saturation.
    fn schedule(mut self: Box<Self>, executor: Arc<CleanupExecutor>) -> Result<(), Box<dyn CleanupJob>> {
        let permit = match executor.reserve_async() {
            Ok(permit) => permit,
            Err(_) => return Err(self),
        };
        permit.spawn(AsyncCleanupTask {
            resource: self.resource.take(),
            context: self.context.take(),
            policy: Some(Arc::clone(&self.policy)),
            future: None,
            registration: self.registration.take(),
            completion: Arc::clone(&self.completion),
            callback: self.callback.take(),
            finished: false,
        });
        Ok(())
    }
}

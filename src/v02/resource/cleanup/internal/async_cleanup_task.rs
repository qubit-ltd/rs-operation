// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use super::super::AsyncCleanup;
use super::super::AsyncCleanupCompletion;
use super::super::CleanupFuture;
use super::super::CleanupOutcome;
use super::super::UnconfirmedCleanup;
use super::super::lock;
use crate::v02::context::CleanupContext;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::report::CleanupEvidence;

/// Drives one asynchronous cleanup policy and records its completion.
///
/// The resource and context move into the policy on the first poll. Policy or
/// future panics are captured as unconfirmed cleanup so the resource owner and
/// registration are not treated as successfully released.
///
/// # Type Parameters
///
/// - `T`: The thread-safe resource type processed by the cleanup policy.
pub(in crate::v02::resource::cleanup) struct AsyncCleanupTask<T: Send + Sync + 'static> {
    /// Resource transferred to the policy when its future is first created.
    pub(in crate::v02::resource::cleanup) resource: Option<T>,
    /// Scope-provided context consumed when cleanup starts.
    pub(in crate::v02::resource::cleanup) context: Option<CleanupContext>,
    /// Policy retained until a completion record is delivered.
    pub(in crate::v02::resource::cleanup) policy: Option<Arc<dyn AsyncCleanup<T>>>,
    /// Policy future, initialized on the first poll and retained while pending.
    pub(in crate::v02::resource::cleanup) future: Option<CleanupFuture<T>>,
    /// Capacity registration transferred to normal or unconfirmed completion.
    pub(in crate::v02::resource::cleanup) registration: Option<ResourceRegistration>,
    /// Shared slot used when no direct callback is configured.
    pub(in crate::v02::resource::cleanup) completion: Arc<Mutex<Option<AsyncCleanupCompletion<T>>>>,
    /// Prevents a completed task from delivering its result more than once.
    pub(in crate::v02::resource::cleanup) finished: bool,
    /// Optional one-shot handler for the completion record.
    pub(in crate::v02::resource::cleanup) callback: Option<Box<dyn FnOnce(AsyncCleanupCompletion<T>) + Send + 'static>>,
}

impl<T: Send + Sync + 'static> Unpin for AsyncCleanupTask<T> {}

impl<T: Send + Sync + 'static> Future for AsyncCleanupTask<T> {
    /// Polling completes after the cleanup result has been delivered.
    type Output = ();

    /// Starts cleanup on the first poll and advances its future thereafter.
    ///
    /// `Pending` preserves the policy future and all required ownership state.
    /// `Ready(())` means a normal or unconfirmed completion record has been
    /// delivered to the callback or shared result slot.
    ///
    /// # Parameters
    ///
    /// - `self`: The cleanup task being advanced.
    /// - `context`: Executor context used to poll the policy future.
    ///
    /// # Returns
    ///
    /// `Pending` while cleanup is incomplete, or `Ready(())` after completion
    /// has been reported.
    ///
    /// # Panics
    ///
    /// Panics from policy creation or future polling are captured as
    /// unconfirmed cleanup. A panic from the completion callback propagates.
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(());
        }
        let outcome = panic::catch_unwind(AssertUnwindSafe(|| {
            if this.future.is_none() {
                let resource = this.resource.take().expect("resource is present");
                let cleanup_context = this.context.take().expect("cleanup context is present");
                let policy = Arc::clone(this.policy.as_ref().expect("policy is present"));
                this.future = Some(policy.cleanup(resource, cleanup_context));
            }
            this.future
                .as_mut()
                .expect("future was initialized")
                .as_mut()
                .poll(context)
        }));
        match outcome {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(outcome)) => {
                let evidence = matches!(&outcome, CleanupOutcome::Released).then_some(CleanupEvidence::Confirmed);
                let policy = this.policy.take().expect("policy is present");
                let completion = AsyncCleanupCompletion {
                    outcome: Some(outcome),
                    evidence,
                    policy,
                    registration: this.registration.take(),
                    unconfirmed: None,
                };
                if let Some(callback) = this.callback.take() {
                    callback(completion);
                } else {
                    *lock(&this.completion) = Some(completion);
                }
                this.future.take();
                this.finished = true;
                Poll::Ready(())
            }
            Err(panic) => {
                let capsule = this
                    .future
                    .take()
                    .map(|future| Box::new(future) as Box<dyn std::any::Any + Send>);
                let policy = this.policy.take().expect("policy is present");
                let policy_capsule: Box<dyn std::any::Any + Send + Sync> = Box::new(Arc::clone(&policy));
                let completion = AsyncCleanupCompletion {
                    outcome: None,
                    evidence: None,
                    policy,
                    registration: None,
                    unconfirmed: Some(UnconfirmedCleanup {
                        registration: this.registration.take(),
                        panic,
                        capsule,
                        policy: Some(policy_capsule),
                    }),
                };
                if let Some(callback) = this.callback.take() {
                    callback(completion);
                } else {
                    *lock(&this.completion) = Some(completion);
                }
                this.finished = true;
                Poll::Ready(())
            }
        }
    }
}

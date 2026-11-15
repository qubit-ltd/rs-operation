// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::CleanupOutcome;
use crate::v02::context::CleanupContext;

/// The boxed future returned by an asynchronous cleanup policy.
///
/// Its output records whether cleanup released or transferred the resource,
/// or returns the resource alongside a failure so an owner can retry cleanup.
/// The `Send + 'static` bounds allow the framework to schedule the future
/// independently of the caller.
///
/// # Type Parameters
///
/// - `T`: The resource type retained when cleanup fails.
///
/// # Examples
///
/// ```
/// use qubit_operation::CleanupFuture;
/// use qubit_operation::CleanupOutcome;
///
/// fn confirmed_release() -> CleanupFuture<String> {
///     Box::pin(async { CleanupOutcome::Released })
/// }
/// ```
pub type CleanupFuture<T> = Pin<Box<dyn Future<Output = CleanupOutcome<T>> + Send + 'static>>;

/// An asynchronous policy that releases or transfers a resource.
///
/// The framework retains the policy through an `Arc` while its cleanup future
/// runs. A failed outcome must return the resource, allowing the owner to keep
/// it available for a later retry.
///
/// # Type Parameters
///
/// - `T`: The resource managed by this cleanup policy. It must be thread-safe
///   and `'static` because the framework may run cleanup on a worker.
///
/// # Examples
///
/// ```
/// use std::sync::Arc;
///
/// use qubit_operation::CleanupContext;
/// use qubit_operation::AsyncCleanup;
/// use qubit_operation::CleanupFuture;
/// use qubit_operation::CleanupOutcome;
///
/// struct ReleaseText;
///
/// impl AsyncCleanup<String> for ReleaseText {
///     fn cleanup(
///         self: Arc<Self>,
///         resource: String,
///         _context: CleanupContext,
///     ) -> CleanupFuture<String> {
///         Box::pin(async move {
///             drop(resource);
///             CleanupOutcome::Released
///         })
///     }
/// }
///
/// fn schedule_release(
///     policy: Arc<ReleaseText>,
///     resource: String,
///     context: CleanupContext,
/// ) -> CleanupFuture<String> {
///     policy.cleanup(resource, context)
/// }
/// ```
pub trait AsyncCleanup<T: Send + Sync + 'static>: Send + Sync + 'static {
    /// Starts cleanup for one resource using the scope-provided context.
    ///
    /// Return [`CleanupOutcome::Failed`] with the resource still owned when
    /// cleanup cannot be confirmed. The returned future is driven by the
    /// framework; dropping it before completion does not imply release.
    ///
    /// # Parameters
    ///
    /// - `self`: Shared policy ownership kept alive while this call runs.
    /// - `resource`: The value whose cleanup is being attempted.
    /// - `context`: Scope-provided cancellation and diagnostic context.
    ///
    /// # Returns
    ///
    /// A sendable future that reports confirmed release, accepted transfer,
    /// or failure with the resource retained.
    fn cleanup(self: Arc<Self>, resource: T, context: CleanupContext) -> CleanupFuture<T>;
}

impl<T, C> AsyncCleanup<T> for Arc<C>
where
    T: Send + Sync + 'static,
    C: AsyncCleanup<T> + ?Sized,
{
    /// Forwards cleanup to the policy held by this shared pointer.
    ///
    /// # Parameters
    ///
    /// - `self`: Shared pointer to the cleanup policy.
    /// - `resource`: Resource whose cleanup is being attempted.
    /// - `context`: Scope-provided cancellation and diagnostic context.
    ///
    /// # Returns
    ///
    /// The policy's future reporting release, transfer, or retained failure.
    fn cleanup(self: Arc<Self>, resource: T, context: CleanupContext) -> CleanupFuture<T> {
        Arc::clone(self.as_ref()).cleanup(resource, context)
    }
}

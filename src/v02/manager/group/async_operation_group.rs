// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;

use super::AsyncOperationHandle;
use super::GroupSnapshot;
use super::OperationGroup;
use super::OperationOptions;
use super::SubmitError;

/// Async submission view over the same group target and local lifecycle state.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationManager;
/// use qubit_operation::{GroupOptions, ManagerOptions};
///
/// let manager = OperationManager::new(ManagerOptions::default()).unwrap();
/// let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
/// let async_group = group.as_async();
/// let sync_group = async_group.as_sync();
/// assert_eq!(sync_group.capacity(), 2);
/// ```
///
/// The async view shares the group's admission policy and lifecycle state.
#[must_use = "use this group view to submit operations or manage its lifecycle"]
#[derive(Clone)]
pub struct AsyncOperationGroup {
    /// Synchronous group view that owns the shared target and lifecycle.
    group: OperationGroup,
}

impl AsyncOperationGroup {
    /// Builds an async view from the existing synchronous group view.
    ///
    /// # Parameters
    ///
    /// - `group`: Existing group view whose target and lifecycle are shared.
    #[inline]
    pub(super) fn from_group(group: OperationGroup) -> Self {
        Self { group }
    }

    /// Returns the corresponding synchronous group view.
    ///
    /// # Parameters
    ///
    /// - `self`: Async group view whose synchronous counterpart is returned.
    ///
    /// # Returns
    ///
    /// A group view sharing this view's coordinator, admission target, and
    /// lifecycle.
    #[must_use = "use the synchronous group view to submit or observe group operations"]
    #[inline]
    pub fn as_sync(&self) -> OperationGroup {
        self.group.clone()
    }

    /// Returns a snapshot of the group's current admission and lifecycle state.
    ///
    /// # Parameters
    ///
    /// - `self`: Group view whose current state is observed.
    ///
    /// # Returns
    ///
    /// The current group snapshot.
    #[must_use]
    #[inline]
    pub fn snapshot(&self) -> GroupSnapshot {
        self.group.snapshot()
    }

    /// Attempts to submit an async operation without waiting for capacity.
    ///
    /// # Type Parameters
    ///
    /// - `F`: Closure that creates the operation future from its scope.
    /// - `Fut`: Future returned by the closure.
    /// - `T`: Successful output of the operation.
    /// - `E`: Operation error value.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission and execution settings for the operation.
    /// - `body`: Closure that receives the operation scope and creates its
    ///   future.
    /// - `self`: Async group view that submits the operation.
    ///
    /// # Returns
    ///
    /// A handle for observing the accepted operation.
    ///
    /// # Errors
    ///
    /// Returns the original body with the admission error when submission
    /// fails.
    #[inline]
    pub fn try_submit<F, Fut, T, E>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<AsyncOperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(crate::v02::scope::OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.group.manager.as_async().try_submit(options, body)
    }

    /// Submits an async operation, awaiting retryable group capacity pressure.
    ///
    /// # Type Parameters
    ///
    /// - `F`: Closure that creates the operation future from its scope.
    /// - `Fut`: Future returned by the closure.
    /// - `T`: Successful output of the operation.
    /// - `E`: Operation error value.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission and execution settings for the operation.
    /// - `body`: Closure that receives the operation scope and creates its
    ///   future.
    /// - `self`: Async group view that waits for capacity and submits the
    ///   operation.
    ///
    /// # Returns
    ///
    /// A handle for observing the accepted operation.
    ///
    /// # Errors
    ///
    /// Returns the original body with the admission or setup error.
    #[inline]
    pub async fn submit<F, Fut, T, E>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<AsyncOperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(crate::v02::scope::OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.group.manager.as_async().submit(options, body).await
    }

    /// Runs an async operation and returns after its outcome becomes available.
    ///
    /// # Type Parameters
    ///
    /// - `F`: Closure that creates the operation future from its scope.
    /// - `Fut`: Future returned by the closure.
    /// - `T`: Successful output of the operation.
    /// - `E`: Operation error value.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission and execution settings for the operation.
    /// - `body`: Closure that receives the operation scope and creates its
    ///   future.
    /// - `self`: Async group view that runs the operation.
    ///
    /// # Returns
    ///
    /// The operation outcome once it is available.
    ///
    /// # Errors
    ///
    /// Returns the original body if admission or operation setup fails.
    #[inline]
    pub async fn run<F, Fut, T, E>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<crate::v02::report::OperationOutcome<T, E>, crate::v02::report::RunError<F>>
    where
        F: FnOnce(crate::v02::scope::OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.group.manager.as_async().run(options, body).await
    }

    /// Runs an async operation and waits for both outcome and settlement.
    ///
    /// # Type Parameters
    ///
    /// - `F`: Closure that creates the operation future from its scope.
    /// - `Fut`: Future returned by the closure.
    /// - `T`: Successful output of the operation.
    /// - `E`: Operation error value.
    ///
    /// # Parameters
    ///
    /// - `options`: Admission and execution settings for the operation.
    /// - `body`: Closure that receives the operation scope and creates its
    ///   future.
    /// - `self`: Async group view that runs and settles the operation.
    ///
    /// # Returns
    ///
    /// The completion containing the outcome and settlement report.
    ///
    /// # Errors
    ///
    /// Returns the original body if admission or operation setup fails.
    #[inline]
    pub async fn run_and_settle<F, Fut, T, E>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<crate::v02::report::OperationCompletion<T, E>, crate::v02::report::RunError<F>>
    where
        F: FnOnce(crate::v02::scope::OperationScope) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.group.manager.as_async().run_and_settle(options, body).await
    }

    /// Creates a service resource scope under this group's manager view.
    ///
    /// # Parameters
    ///
    /// - `options`: Capacity and lifecycle configuration for the resource
    ///   scope.
    /// - `self`: Group view whose manager creates the service resource scope.
    ///
    /// # Returns
    ///
    /// The created service resource scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the resource scope cannot be created.
    #[inline]
    pub fn resource_scope(
        &self,
        options: crate::v02::report::ServiceResourceOptions,
    ) -> Result<crate::v02::resource::service::ServiceResourceScope, crate::v02::report::ResourceScopeError> {
        self.group.resource_scope(options)
    }

    /// Requests shutdown for the shared manager and returns its async observer.
    ///
    /// # Parameters
    ///
    /// - `mode`: Whether to drain accepted work or request cancellation.
    /// - `self`: Group view whose manager receives the shutdown request.
    ///
    /// # Returns
    ///
    /// A handle that observes completion of the global shutdown request.
    #[must_use = "retain this handle to observe shutdown completion"]
    #[inline]
    pub fn request_shutdown(
        &self,
        mode: crate::v02::report::ShutdownMode,
    ) -> crate::v02::manager::shutdown::AsyncShutdownHandle {
        self.group.request_shutdown(mode).as_async()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use crate::OperationOptions;
    use crate::v02::manager::core::OperationManager;
    use crate::v02::report::GroupOptions;
    use crate::v02::report::ManagerOptions;
    use crate::v02::report::OperationOutcome;
    use crate::v02::report::ShutdownMode;

    fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
        let mut context = Context::from_waker(Waker::noop());
        future.poll(&mut context)
    }

    fn block_on<F: Future>(mut future: Pin<&mut F>) -> F::Output {
        loop {
            match poll_once(future.as_mut()) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn test_async_group_submit_run_and_run_and_settle_share_group_target() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let group = manager.group(GroupOptions { capacity: 2 }).unwrap().as_async();
        assert_eq!(group.snapshot().active_operations, 0);
        assert_eq!(group.as_sync().capacity(), 2);

        let mut submit = Box::pin(group.submit(OperationOptions::default(), |_| async { Ok::<_, ()>(11usize) }));
        let operation = match poll_once(submit.as_mut()) {
            Poll::Ready(Ok(operation)) => operation,
            _ => panic!("uncontended group submission should complete immediately"),
        };
        assert!(matches!(
            operation.into_sync().wait_outcome().unwrap(),
            OperationOutcome::Succeeded(11)
        ));

        let mut run = Box::pin(group.run(OperationOptions::default(), |_| async { Ok::<_, ()>(13usize) }));
        assert!(matches!(
            block_on(run.as_mut()).unwrap(),
            OperationOutcome::Succeeded(13)
        ));

        let mut settled =
            Box::pin(group.run_and_settle(OperationOptions::default(), |_| async { Ok::<_, ()>(17usize) }));
        let completion = block_on(settled.as_mut()).unwrap();
        assert!(matches!(completion.outcome, OperationOutcome::Succeeded(17)));
        assert!(completion.settlement.all_released());

        let shutdown = group.request_shutdown(ShutdownMode::Drain);
        let mut wait = Box::pin(shutdown.wait());
        assert_eq!(block_on(wait.as_mut()).snapshot.active_operations, 0);
    }
}

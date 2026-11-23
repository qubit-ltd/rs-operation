// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::sync::Arc;

use super::AsyncOperationGroup;
use super::AsyncOperationHandle;
use super::GroupCore;
use super::GroupOptions;
use super::GroupSnapshot;
use super::ManagerCreateError;
use super::OperationHandle;
use super::OperationManager;
use super::OperationOptions;
use super::SubmitError;
use crate::v02::resource::RecoveryHandle;

/// A bounded submission group with its own admission and shutdown boundary.
///
/// The group routes accepted operations through one shared policy while keeping
/// descendants of already accepted operations associated with their parent
/// scope.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationManager;
/// use qubit_operation::{GroupOptions, ManagerOptions};
///
/// let manager = OperationManager::new(ManagerOptions::default()).unwrap();
/// let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
/// assert_eq!(group.capacity(), 2);
/// group.close_admission();
/// ```
#[must_use = "use this group view to submit operations or manage its lifecycle"]
#[derive(Clone)]
pub struct OperationGroup {
    /// Manager view bound to this group's admission policy.
    pub(super) manager: OperationManager,
    /// Shared group state used for admission, snapshots, and shutdown.
    pub(super) core: Arc<GroupCore>,
    /// Maximum number of operations admitted concurrently by this group.
    capacity: usize,
}

impl OperationGroup {
    /// Creates a group view after validating its operation capacity.
    ///
    /// The returned manager is routed through the new group's shared policy.
    ///
    /// # Parameters
    ///
    /// - `manager`: Parent manager view whose coordinator is shared with the
    ///   group.
    /// - `options`: Capacity limit used for concurrent group operations.
    ///
    /// # Returns
    ///
    /// A group whose submissions use the configured capacity.
    ///
    /// # Errors
    ///
    /// Returns `InvalidOptions` if the configured capacity is zero.
    pub(crate) fn new(manager: OperationManager, options: GroupOptions) -> Result<Self, ManagerCreateError> {
        if options.capacity == 0 {
            return Err(ManagerCreateError::InvalidOptions {
                field: "group.capacity",
            });
        }
        let core = GroupCore::new(options.capacity);
        let manager = manager.with_group_core(Arc::clone(&core));
        Ok(Self {
            manager,
            core,
            capacity: options.capacity,
        })
    }

    /// Returns the shared manager coordinator used by this group.
    pub(crate) fn manager_core(&self) -> Arc<crate::v02::manager::core::ManagerCore> {
        self.manager.core_arc()
    }

    /// Returns the shared admission state for this group.
    pub(crate) fn group_core(&self) -> Arc<GroupCore> {
        Arc::clone(&self.core)
    }

    /// Returns the configured concurrent operation limit for this group.
    ///
    /// # Returns
    ///
    /// The maximum number of concurrently admitted operations.
    #[must_use]
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Creates an async view sharing this group's coordinator and limits.
    ///
    /// # Returns
    ///
    /// An async group view bound to the same admission policy.
    #[must_use = "use the async view to submit or observe group operations"]
    #[inline]
    pub fn as_async(&self) -> AsyncOperationGroup {
        AsyncOperationGroup::from_group(self.clone())
    }

    /// Returns the current counters and shutdown state for the group.
    ///
    /// # Returns
    ///
    /// A point-in-time snapshot of group admission and shutdown state.
    #[must_use]
    #[inline]
    pub fn snapshot(&self) -> GroupSnapshot {
        self.core.public_snapshot()
    }

    /// Returns the number of group operations whose settlement has not
    /// completed.
    ///
    /// The count includes accepted reservations that are still in submission
    /// preflight, even before their operation scopes are registered. It drops
    /// when settlement completes, independently of whether the typed outcome
    /// has been observed.
    ///
    /// # Returns
    ///
    /// The current count of unsettled group operations.
    #[must_use = "inspect unsettled group operations"]
    #[inline]
    pub fn unsettled_operation_count(&self) -> usize {
        self.core.unsettled_operation_count()
    }

    /// Returns a point-in-time snapshot of unresolved recovery handles owned
    /// by this group.
    ///
    /// The snapshot includes recoveries created by operations submitted
    /// directly to this group and its descendants. Handles are unique by
    /// recovery identity and remain available until they are resolved or
    /// transferred to another recovery owner. Each call returns a new snapshot
    /// of the group's current recovery ledger.
    ///
    /// # Returns
    ///
    /// A complete list of unresolved recovery handles currently owned by this
    /// group.
    #[must_use = "inspect the group's unresolved recovery handles"]
    #[inline]
    pub fn recovery_handles_snapshot(&self) -> Vec<RecoveryHandle> {
        self.core.recovery_handles_snapshot()
    }

    /// Stops direct group submissions while allowing already accepted
    /// operations to submit descendants through their bound scope views.
    ///
    /// This is idempotent and does not cancel accepted operations.
    #[inline]
    pub fn close_admission(&self) {
        self.core.close();
    }

    /// Attempts to submit a synchronous body without waiting for capacity.
    ///
    /// Rejected submissions return the original body to the caller.
    ///
    /// # Type Parameters
    ///
    /// - `F`: One-shot synchronous body receiving its operation scope.
    /// - `T`: Value returned by the body.
    /// - `E`: Error returned by the body.
    ///
    /// # Parameters
    ///
    /// - `options`: Resource and execution options for the operation.
    /// - `body`: Synchronous operation body.
    ///
    /// # Returns
    ///
    /// A handle for the accepted operation or the original body and rejection
    /// reason.
    ///
    /// # Errors
    ///
    /// The `Err` value retains `body` when admission rejects the operation.
    #[inline]
    pub fn try_submit<F, T, E>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(crate::v02::scope::OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.manager.try_submit(options, body)
    }

    /// Submits a synchronous body, waiting for retryable capacity pressure.
    ///
    /// Rejected non-capacity errors return the original body to the caller.
    ///
    /// # Type Parameters
    ///
    /// - `F`: One-shot synchronous body receiving its operation scope.
    /// - `T`: Value returned by the body.
    /// - `E`: Error returned by the body.
    ///
    /// # Parameters
    ///
    /// - `options`: Resource and execution options for the operation.
    /// - `body`: Synchronous operation body, retained while capacity is
    ///   retried.
    ///
    /// # Returns
    ///
    /// A handle for the accepted operation.
    ///
    /// # Errors
    ///
    /// Returns the original body when admission fails for a non-retryable
    /// reason.
    #[inline]
    pub fn submit<F, T, E>(&self, options: OperationOptions, body: F) -> Result<OperationHandle<T, E>, SubmitError<F>>
    where
        F: FnOnce(crate::v02::scope::OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.manager.submit(options, body)
    }

    /// Attempts to submit an async body without waiting for capacity.
    ///
    /// The returned handle observes the outcome while settlement may continue.
    ///
    /// # Type Parameters
    ///
    /// - `F`: One-shot closure that creates the async body from its scope.
    /// - `Fut`: Future returned by the body closure.
    /// - `T`: Value produced by the future.
    /// - `E`: Error produced by the future.
    ///
    /// # Parameters
    ///
    /// - `options`: Resource and execution options for the operation.
    /// - `body`: Closure that receives the operation scope and returns its
    ///   future.
    ///
    /// # Returns
    ///
    /// A handle for the accepted async operation.
    ///
    /// # Errors
    ///
    /// Returns the original body when admission rejects the operation.
    #[inline]
    pub async fn try_submit_async<F, Fut, T, E>(
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
        self.manager.as_async().try_submit(options, body)
    }

    /// Submits a synchronous body and waits until its outcome is observable.
    ///
    /// Resource settlement can continue after this method returns.
    ///
    /// # Type Parameters
    ///
    /// - `F`: One-shot synchronous body receiving its operation scope.
    /// - `T`: Value returned by the body.
    /// - `E`: Error returned by the body.
    ///
    /// # Parameters
    ///
    /// - `options`: Resource and execution options for the operation.
    /// - `body`: Synchronous operation body.
    ///
    /// # Returns
    ///
    /// The operation outcome after it is published.
    ///
    /// # Errors
    ///
    /// Returns submission or outcome-observation failure, retaining the body
    /// when submission is rejected.
    #[inline]
    pub fn run<F, T, E>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<crate::v02::report::OperationOutcome<T, E>, crate::v02::report::RunError<F>>
    where
        F: FnOnce(crate::v02::scope::OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.manager.run(options, body)
    }

    /// Submits a synchronous body and waits for both outcome and settlement.
    ///
    /// # Type Parameters
    ///
    /// - `F`: One-shot synchronous body receiving its operation scope.
    /// - `T`: Value returned by the body.
    /// - `E`: Error returned by the body.
    ///
    /// # Parameters
    ///
    /// - `options`: Resource and execution options for the operation.
    /// - `body`: Synchronous operation body.
    ///
    /// # Returns
    ///
    /// The operation outcome together with its settlement report.
    ///
    /// # Errors
    ///
    /// Returns submission, outcome-observation, or settlement-observation
    /// failure.
    #[inline]
    pub fn run_and_settle<F, T, E>(
        &self,
        options: OperationOptions,
        body: F,
    ) -> Result<crate::v02::report::OperationCompletion<T, E>, crate::v02::report::RunError<F>>
    where
        F: FnOnce(crate::v02::scope::OperationScope) -> Result<T, E> + Send + 'static,
        T: Send + 'static,
        E: Send + 'static,
    {
        self.manager.run_and_settle(options, body)
    }

    /// Creates a service resource scope using this group's capacity policy.
    ///
    /// # Parameters
    ///
    /// - `options`: Service resource scope capacity and behavior configuration.
    ///
    /// # Returns
    ///
    /// A resource scope registered with this group's capacity policy.
    ///
    /// # Errors
    ///
    /// Returns `ResourceScopeError` when scope creation is rejected.
    #[inline]
    pub fn resource_scope(
        &self,
        options: crate::v02::report::ServiceResourceOptions,
    ) -> Result<crate::v02::resource::service::ServiceResourceScope, crate::v02::report::ResourceScopeError> {
        self.manager.service_resource_scope(options.capacity)
    }

    /// Requests group shutdown and returns a handle for observing completion.
    ///
    /// # Parameters
    ///
    /// - `mode`: Whether accepted operations drain or receive cancellation
    ///   requests.
    ///
    /// # Returns
    ///
    /// A group shutdown handle that observes operation and cleanup completion.
    #[inline]
    pub fn request_shutdown(
        &self,
        mode: crate::v02::report::ShutdownMode,
    ) -> crate::v02::manager::shutdown::ShutdownHandle {
        self.core.begin_shutdown(mode);
        crate::v02::manager::shutdown::ShutdownHandle::for_group(self.manager.core_arc(), Arc::clone(&self.core))
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use super::GroupOptions;
    use super::OperationManager;
    use super::OperationOptions;
    use crate::v02::context::CleanupContext;
    use crate::v02::report::CleanupFailure;
    use crate::v02::report::CleanupStage;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ManagerOptions;
    use crate::v02::report::OperationOutcome;
    use crate::v02::report::RecoveryStoreOptions;
    use crate::v02::report::ResourceId;
    use crate::v02::report::ResourceScopeOptions;
    use crate::v02::report::ServiceResourceOptions;
    use crate::v02::report::ShutdownMode;
    use crate::v02::report::ShutdownWait;
    use crate::v02::resource::AsyncCleanup;
    use crate::v02::resource::CleanupFuture;
    use crate::v02::resource::CleanupOutcome;
    use crate::v02::resource::RecoveryStore;

    struct FailCleanup;

    impl AsyncCleanup<usize> for FailCleanup {
        fn cleanup(self: Arc<Self>, resource: usize, _context: CleanupContext) -> CleanupFuture<usize> {
            Box::pin(std::future::ready(CleanupOutcome::Failed {
                failure: CleanupFailure {
                    resource_id: ResourceId::new(ManagerId::new_unique(), 1, 1),
                    stage: CleanupStage::AsyncRelease,
                    summary: DiagnosticSummary::new("retry in recovery test"),
                    source: None,
                },
                resource,
            }))
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn test_sync_group_submission_helpers_share_capacity_and_service_scope() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
        let mut first = group
            .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(3usize))
            .unwrap();
        assert!(matches!(first.wait_outcome().unwrap(), OperationOutcome::Succeeded(3)));
        first.wait_settlement().unwrap();

        let mut submitted = group
            .submit(OperationOptions::default(), |_| Ok::<_, ()>(5usize))
            .unwrap();
        assert!(matches!(
            submitted.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(5)
        ));
        submitted.wait_settlement().unwrap();

        let async_handle =
            block_on(group.try_submit_async(OperationOptions::default(), |_| async { Ok::<_, ()>(7usize) })).unwrap();
        let mut async_handle = async_handle.into_sync();
        assert!(matches!(
            async_handle.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(7)
        ));
        async_handle.wait_settlement().unwrap();

        assert!(matches!(
            group
                .run(OperationOptions::default(), |_| Ok::<_, ()>(11usize))
                .unwrap(),
            OperationOutcome::Succeeded(11)
        ));
        let completion = group
            .run_and_settle(OperationOptions::default(), |_| Ok::<_, ()>(13usize))
            .unwrap();
        assert!(matches!(completion.outcome, OperationOutcome::Succeeded(13)));
        assert!(completion.settlement.all_released());

        let service = group.resource_scope(ServiceResourceOptions { capacity: 1 }).unwrap();
        assert_eq!(group.snapshot().service_resource_scopes, 1);
        service.close();
        assert!(
            group
                .request_shutdown(crate::v02::report::ShutdownMode::Drain)
                .wait_for(std::time::Duration::from_secs(2))
                .is_ok()
        );
    }

    #[test]
    fn test_close_admission_rejects_new_roots() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
        group.close_admission();
        let rejected = group.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
        assert!(matches!(rejected, Err(error) if error.reason() == crate::v02::report::AdmissionError::Closed));
    }

    #[test]
    fn test_group_shutdown_tracks_and_releases_failed_cleanup_recovery() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
        let completion = group
            .run_and_settle(OperationOptions::default(), |scope| {
                let resources = scope.resource_scope(ResourceScopeOptions { capacity: 1 }).unwrap();
                let resource = resources.manage_with(19usize, Arc::new(FailCleanup)).unwrap();
                drop(resource);
                Ok::<_, ()>(())
            })
            .unwrap();
        assert_eq!(completion.settlement.recovery_count, 1);

        let shutdown = group.request_shutdown(ShutdownMode::Drain);
        assert_eq!(shutdown.snapshot().recovery_groups, 1);
        let recovery = completion.settlement.recoveries[0].clone();
        let store = RecoveryStore::new(RecoveryStoreOptions::default()).unwrap();
        let transfer = recovery.transfer_to(&store).unwrap();
        assert_eq!(transfer.recovery, recovery.id());
        assert!(store.contains(recovery.id()));
        assert!(matches!(
            shutdown.wait_for(std::time::Duration::from_secs(2)).unwrap(),
            ShutdownWait::Completed(_)
        ));
    }

    #[test]
    fn test_group_recovery_snapshot_includes_direct_child_recovery_after_handle_drop() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
        let recovery_id = Arc::new(Mutex::new(None));
        let expected_recovery_id = Arc::clone(&recovery_id);

        let parent = group
            .run_and_settle(OperationOptions::default(), move |scope| {
                let child = scope
                    .manager()
                    .run_and_settle(OperationOptions::default(), |child_scope| {
                        let resources = child_scope
                            .resource_scope(ResourceScopeOptions { capacity: 1 })
                            .unwrap();
                        let resource = resources.manage_with(23usize, Arc::new(FailCleanup)).unwrap();
                        drop(resource);
                        Ok::<_, ()>(())
                    })
                    .unwrap();
                assert_eq!(child.settlement.recovery_count, 1);
                *expected_recovery_id.lock().unwrap() = Some(child.settlement.recoveries[0].id());
                Ok::<_, ()>(())
            })
            .unwrap();
        assert!(parent.settlement.all_released());

        let sync_snapshot = group.recovery_handles_snapshot();
        assert_eq!(sync_snapshot.len(), 1);
        assert_eq!(sync_snapshot[0].id(), recovery_id.lock().unwrap().unwrap());

        let async_snapshot = group.as_async().recovery_handles_snapshot();
        assert_eq!(async_snapshot.len(), 1);
        assert_eq!(async_snapshot[0].id(), sync_snapshot[0].id());

        let shutdown = group.request_shutdown(ShutdownMode::Drain);
        let store = RecoveryStore::new(RecoveryStoreOptions::default()).unwrap();
        sync_snapshot[0].transfer_to(&store).unwrap();
        assert!(group.recovery_handles_snapshot().is_empty());
        assert!(matches!(
            shutdown.wait_for(std::time::Duration::from_secs(2)).unwrap(),
            ShutdownWait::Completed(_)
        ));
    }
}

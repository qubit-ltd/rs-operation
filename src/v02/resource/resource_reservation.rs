use std::sync::Arc;

use super::core::ScopeCore;
use super::resource_error::ResourceCommitError;
use super::resource_error::ResourceCommitWithError;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::report::ResourceId;
use crate::v02::resource::AsyncCleanup;
use crate::v02::resource::ResourceRef;
use crate::v02::resource::cleanup_port::CleanupDispatchPermit;
use crate::v02::scope::ScopeCore as OperationScopeCore;

/// Capacity and cleanup queue storage reserved for one resource in one scope.
pub struct ResourceReservation {
    pub(super) scope: Arc<ScopeCore>,
    pub(super) operation_scope: Option<Arc<OperationScopeCore>>,
    pub(super) registration: Option<ResourceRegistration>,
    pub(super) cleanup_permit: Option<Box<dyn CleanupDispatchPermit>>,
    pub(super) tracked: bool,
}
impl ResourceReservation {
    pub(super) fn new(
        scope: Arc<ScopeCore>,
        registration: ResourceRegistration,
        cleanup_permit: Box<dyn CleanupDispatchPermit>,
    ) -> Self {
        Self {
            scope,
            operation_scope: None,
            registration: Some(registration),
            cleanup_permit: Some(cleanup_permit),
            tracked: true,
        }
    }

    pub(crate) fn bind_operation_scope(mut self, scope: Arc<OperationScopeCore>) -> Self {
        self.operation_scope = Some(scope);
        self
    }

    /// Commit a resource to the scope that created this reservation.
    pub fn commit<T: Send + Sync + 'static>(self, value: T) -> Result<ResourceRef<T>, ResourceCommitError<T>> {
        let scope = Arc::clone(&self.scope);
        if let Some(operation_scope) = self.operation_scope.as_ref().cloned() {
            let mut value = Some(value);
            let mut reservation = Some(self);
            match operation_scope.with_resource_lifecycle(|| {
                scope.commit_reserved(
                    value.take().expect("commit value is available"),
                    reservation.take().expect("reservation is available"),
                )
            }) {
                Ok(result) => result,
                Err(reason) => Err(ResourceCommitError::new(
                    reason,
                    value.expect("rejected commit retains resource"),
                    reservation.expect("rejected commit retains reservation"),
                )),
            }
        } else {
            scope.commit_reserved(value, self)
        }
    }

    pub fn commit_with<T, C>(self, value: T, cleanup: C) -> Result<ResourceRef<T>, ResourceCommitWithError<T, C>>
    where
        T: Send + Sync + 'static,
        C: AsyncCleanup<T>,
    {
        let scope = Arc::clone(&self.scope);
        if let Some(operation_scope) = self.operation_scope.as_ref().cloned() {
            let mut value = Some(value);
            let mut cleanup = Some(cleanup);
            let mut reservation = Some(self);
            match operation_scope.with_resource_lifecycle(|| {
                scope.commit_reserved_with(
                    value.take().expect("commit value is available"),
                    cleanup.take().expect("cleanup strategy is available"),
                    reservation.take().expect("reservation is available"),
                )
            }) {
                Ok(result) => result,
                Err(reason) => Err(ResourceCommitWithError::new(
                    reason,
                    value.expect("rejected commit retains resource"),
                    cleanup.expect("rejected commit retains cleanup strategy"),
                    reservation.expect("rejected commit retains reservation"),
                )),
            }
        } else {
            scope.commit_reserved_with(value, cleanup, self)
        }
    }

    /// Transfers a rejected resource into this scope's reserved cleanup path.
    pub fn retain_for_cleanup<T: Send + Sync + 'static>(self, value: T) -> ResourceId {
        Arc::clone(&self.scope).retain_reserved(value, self)
    }

    /// Transfers a rejected resource and its strategy into reserved cleanup.
    pub fn retain_with_for_cleanup<T, C>(self, value: T, cleanup: C) -> ResourceId
    where
        T: Send + Sync + 'static,
        C: AsyncCleanup<T>,
    {
        Arc::clone(&self.scope).retain_reserved_with(value, cleanup, self)
    }
}

impl Drop for ResourceReservation {
    fn drop(&mut self) {
        if !self.tracked {
            return;
        }
        // Return both permits before releasing the pending completion count.
        drop(self.cleanup_permit.take());
        drop(self.registration.take());
        self.scope.release_reservation();
        self.tracked = false;
    }
}

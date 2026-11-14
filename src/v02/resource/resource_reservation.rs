use super::core::ScopeCore;
use super::resource_error::ResourceCommitError;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::resource::ResourceRef;
use crate::v02::resource::cleanup_port::CleanupDispatchPermit;
use std::sync::Arc;

/// Capacity and cleanup queue storage reserved for one resource in one scope.
pub struct ResourceReservation {
    pub(super) scope: Arc<ScopeCore>,
    pub(super) registration: Option<ResourceRegistration>,
    pub(super) cleanup_permit: Option<Box<dyn CleanupDispatchPermit>>,
}
impl ResourceReservation {
    pub(super) fn new(
        scope: Arc<ScopeCore>,
        registration: ResourceRegistration,
        cleanup_permit: Box<dyn CleanupDispatchPermit>,
    ) -> Self {
        Self {
            scope,
            registration: Some(registration),
            cleanup_permit: Some(cleanup_permit),
        }
    }
    /// Commit a resource to the scope that created this reservation.
    pub fn commit<T: Send + Sync + 'static>(
        self,
        value: T,
    ) -> Result<ResourceRef<T>, ResourceCommitError<T>> {
        let scope = Arc::clone(&self.scope);
        scope.commit_reserved(value, self)
    }
}

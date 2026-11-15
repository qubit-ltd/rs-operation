// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

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
///
/// A reservation holds both admission capacity and a cleanup dispatch permit.
/// Commit it with [`commit`](Self::commit) or
/// [`commit_with`](Self::commit_with) to create a managed resource. If it is
/// dropped without being consumed, both permits are returned to the scope.
pub struct ResourceReservation {
    /// Resource scope that owns the reserved capacity.
    pub(super) scope: Arc<ScopeCore>,
    /// Operation scope whose lifecycle must admit the commit, when bound.
    pub(super) operation_scope: Option<Arc<OperationScopeCore>>,
    /// Capacity registration retained until commit or drop.
    pub(super) registration: Option<ResourceRegistration>,
    /// Cleanup queue permit retained until commit or drop.
    pub(super) cleanup_permit: Option<Box<dyn CleanupDispatchPermit>>,
    /// Whether this reservation still contributes to pending completion.
    pub(super) tracked: bool,
}
impl ResourceReservation {
    /// Creates a reservation for a resource scope and its cleanup queue.
    ///
    /// # Parameters
    ///
    /// * `scope` — resource scope that owns the reservation.
    /// * `registration` — capacity registration reserved for one resource.
    /// * `cleanup_permit` — cleanup queue slot reserved for that resource.
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

    /// Commits a value as a resource in the scope that created this
    /// reservation.
    ///
    /// If the bound operation scope has ended, the error retains the value and
    /// reservation so the caller can retry or route the value to cleanup.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value type; it must be safe to share across threads.
    ///
    /// # Parameters
    ///
    /// * `value` — resource value to register in the reserved scope.
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

    /// Commits a value with its asynchronous cleanup strategy.
    ///
    /// If admission fails, the returned error retains the value, cleanup
    /// strategy, and reservation.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value type; it must be safe to share across threads.
    /// * `C` — asynchronous cleanup strategy for values of type `T`.
    ///
    /// # Parameters
    ///
    /// * `value` — resource value to register in the reserved scope.
    /// * `cleanup` — cleanup strategy used when the resource is released.
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

    /// Routes a value that could not be committed through the reserved cleanup
    /// path.
    ///
    /// Returns the identity assigned to the retained resource.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value type; it must be safe to share across threads.
    ///
    /// # Parameters
    ///
    /// * `value` — rejected resource value to route through reserved cleanup.
    #[inline]
    pub fn retain_for_cleanup<T: Send + Sync + 'static>(self, value: T) -> ResourceId {
        Arc::clone(&self.scope).retain_reserved(value, self)
    }

    /// Routes a rejected value and its cleanup strategy through reserved
    /// cleanup.
    ///
    /// Returns the identity assigned to the retained resource.
    ///
    /// # Type Parameters
    ///
    /// * `T` — resource value type; it must be safe to share across threads.
    /// * `C` — asynchronous cleanup strategy for values of type `T`.
    ///
    /// # Parameters
    ///
    /// * `value` — rejected resource value to route through reserved cleanup.
    /// * `cleanup` — cleanup strategy to use for the rejected value.
    #[inline]
    pub fn retain_with_for_cleanup<T, C>(self, value: T, cleanup: C) -> ResourceId
    where
        T: Send + Sync + 'static,
        C: AsyncCleanup<T>,
    {
        Arc::clone(&self.scope).retain_reserved_with(value, cleanup, self)
    }

    /// Associates an operation scope whose lifecycle guards the eventual
    /// commit.
    ///
    /// # Parameters
    ///
    /// * `scope` — operation scope that must still admit the eventual commit.
    pub(crate) fn bind_operation_scope(mut self, scope: Arc<OperationScopeCore>) -> Self {
        self.operation_scope = Some(scope);
        self
    }
}

impl Drop for ResourceReservation {
    /// Returns unused capacity and cleanup queue permits to the scope.
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

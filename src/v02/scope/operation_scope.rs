// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::OperationResourceAuthority;
use super::ScopeCore;
use super::operation_resource_authority::reserve_resource_for_core;
use crate::v02::context::OperationContext;
use crate::v02::manager::core::ManagerCore;
use crate::v02::manager::core::OperationManager;
use crate::v02::report::OperationId;
#[cfg(test)]
use crate::v02::report::OperationOutcome;
use crate::v02::report::StopReason;

/// Non-cloneable authority scoped to one operation body.
///
/// The manager passes a scope to each operation body. Its manager view submits
/// direct children, and resource registration remains bounded by inherited
/// capacity until scope end and outstanding user references permit cleanup.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationScope;
///
/// // The manager supplies this value to an operation body.
/// fn start_child(scope: &OperationScope) {
///     let child_manager = scope.manager();
///     let _ = child_manager.try_submit(Default::default(), |_| Ok::<_, ()>(()));
/// }
/// ```
pub struct OperationScope {
    /// Internal scope state shared with child and cleanup callbacks.
    pub(crate) core: Arc<ScopeCore>,
    /// Manager coordinator used for submissions made from this body.
    manager: Arc<ManagerCore>,
}

impl OperationScope {
    /// Builds the body-scoped authority from manager-owned state.
    ///
    /// # Parameters
    ///
    /// * `core` — the operation's scope state.
    /// * `manager` — the manager coordinator used by this operation body.
    ///
    /// # Returns
    ///
    /// A body-scoped authority retaining the scope and manager state.
    #[must_use = "retain the operation scope authority for the body lifetime"]
    pub(crate) fn new(core: Arc<ScopeCore>, manager: Arc<ManagerCore>) -> Self {
        Self { core, manager }
    }

    /// Returns this operation's stable identifier.
    ///
    /// # Returns
    ///
    /// The identifier shared by this operation and its reports.
    #[must_use]
    pub fn id(&self) -> OperationId {
        self.core.id()
    }

    /// Returns the operation's shared cancellation, clock, and budget context.
    ///
    /// # Returns
    ///
    /// A clone of the context handle, sharing the operation's cancellation and
    /// limits.
    #[must_use]
    pub fn context(&self) -> OperationContext {
        self.core.context().clone()
    }

    /// Returns weak authority for a resource reservation made later.
    ///
    /// The returned value does not keep this operation alive and consumes no
    /// resource capacity until its `reserve_resource` method is called.
    #[must_use]
    pub fn resource_authority(&self) -> OperationResourceAuthority {
        OperationResourceAuthority::new(&self.core)
    }

    /// Creates an owned manager view bound to this operation's direct children.
    ///
    /// The view owns manager state but retains a weak parent target;
    /// submissions fail after this scope ends.
    ///
    /// # Returns
    ///
    /// An owned manager view restricted to this scope's direct children.
    pub fn manager(&self) -> OperationManager {
        let manager = self
            .core
            .manager()
            .upgrade()
            .unwrap_or_else(|| Arc::clone(&self.manager));
        OperationManager::bound(manager, &self.core)
    }

    /// Creates an independently bounded resource scope attached to this
    /// operation. Its cleanup report participates in operation settlement.
    ///
    /// # Parameters
    ///
    /// `options.capacity` limits resource entries in the child scope, while
    /// inherited `ScopeLimits` continue to constrain the entire hierarchy.
    ///
    /// # Returns
    ///
    /// Returns the attached resource scope on success.
    ///
    /// # Errors
    ///
    /// Returns an error when the operation ended or stopped, the capacity is
    /// invalid, or the scope-instance quota is exhausted.
    pub fn resource_scope(
        &self,
        options: crate::v02::report::ResourceScopeOptions,
    ) -> Result<crate::v02::resource::ResourceScope, crate::v02::report::ResourceScopeError> {
        self.manager.operation_resource_scope(&self.core, options)
    }

    /// Registers a resource in this operation's real cleanup scope.
    ///
    /// Cleanup waits for both scope end and all user references to be released.
    ///
    /// # Type Parameters
    ///
    /// * `T` — the resource type, which must be safe to share across threads.
    ///
    /// # Parameters
    ///
    /// * `resource` — the value to retain until cleanup completes.
    ///
    /// # Returns
    ///
    /// A reference-counted handle to the registered resource.
    ///
    /// # Errors
    ///
    /// Returns the original resource when its scope ended, stopped, or reached
    /// its resource-entry capacity before registration.
    pub fn manage<T: Send + Sync + 'static>(
        &self,
        resource: T,
    ) -> Result<crate::v02::resource::ResourceRef<T>, crate::v02::resource::ManageResourceError<T>> {
        let reservation = match self.reserve_resource() {
            Ok(reservation) => reservation,
            Err(error) => {
                let kind = match error {
                    crate::v02::resource::ResourceCapacityError::ScopeEnded => {
                        crate::v02::report::ResourceManageErrorKind::ScopeEnded
                    }
                    crate::v02::resource::ResourceCapacityError::Stopped => {
                        crate::v02::report::ResourceManageErrorKind::Stopped
                    }
                    crate::v02::resource::ResourceCapacityError::CapacityExceeded => {
                        crate::v02::report::ResourceManageErrorKind::CapacityExceeded
                    }
                };
                return Err(crate::v02::resource::ManageResourceError::new(kind, resource));
            }
        };
        reservation.commit(resource).map_err(|error| {
            let (kind, resource, reservation) = error.into_parts();
            drop(reservation);
            crate::v02::resource::ManageResourceError::new(kind, resource)
        })
    }

    /// Reserves scope capacity and cleanup storage before creating a resource.
    ///
    /// The reservation keeps cleanup storage available even when the operation
    /// ends before a resource is committed.
    ///
    /// # Returns
    ///
    /// A reservation that can commit one resource into this operation's scope.
    ///
    /// # Errors
    ///
    /// Returns `ScopeEnded`, `Stopped`, or `CapacityExceeded` without creating
    /// a resource.
    pub fn reserve_resource(
        &self,
    ) -> Result<crate::v02::resource::ResourceReservation, crate::v02::resource::ResourceCapacityError> {
        reserve_resource_for_core(Arc::clone(&self.core))
    }

    /// Looks up a resource while the attached scope remains open.
    ///
    /// # Type Parameters
    ///
    /// * `T` — the resource type expected for this key.
    ///
    /// # Parameters
    ///
    /// * `key` — the typed key identifying the resource.
    ///
    /// # Returns
    ///
    /// A reference-counted handle to the matching resource.
    ///
    /// # Errors
    ///
    /// Returns an error for an ended or stopped scope, a wrong-scope key, or a
    /// stale resource generation.
    pub fn get_resource<T: Send + Sync + 'static>(
        &self,
        key: crate::v02::resource::ResourceKey<T>,
    ) -> Result<crate::v02::resource::ResourceRef<T>, crate::v02::report::ResourceLookupError> {
        self.core.resource_scope().get_resource(key)
    }

    /// Requests stop for this scope and propagates it to descendants.
    ///
    /// # Parameters
    ///
    /// `reason` records why this operation and its descendants are stopping.
    ///
    /// # Returns
    ///
    /// Returns [`StopRequestStatus::Accepted`](crate::v02::report::StopRequestStatus::Accepted)
    /// for a new request,
    /// [`StopRequestStatus::AlreadyRequested`](crate::v02::report::StopRequestStatus::AlreadyRequested)
    /// when a stop request already exists, or
    /// [`StopRequestStatus::AlreadyFinished`](crate::v02::report::StopRequestStatus::AlreadyFinished)
    /// after the scope has ended.
    #[must_use]
    pub fn request_stop(&self, reason: StopReason) -> crate::v02::report::StopRequestStatus {
        self.core.request_stop_status(reason)
    }

    /// Records an application-reported effect within the inherited effect
    /// record budget. Capacity exhaustion preserves prior records and
    /// increments the visible overflow count. Records are application claims;
    /// they do not prove rollback or release.
    ///
    /// # Parameters
    ///
    /// * `record` — application-reported effect to add to this operation's
    ///   report.
    pub fn record_effect(
        &self,
        record: crate::v02::report::EffectRecord,
    ) -> Result<(), crate::v02::report::EffectRecordError> {
        self.core.record_effect(record)
    }

    /// Resolves the body result against required-child failures and stop state.
    ///
    /// # Type Parameters
    ///
    /// * `T` — success value returned by the operation body.
    /// * `E` — error returned by the operation body.
    ///
    /// # Parameters
    ///
    /// * `result` — operation body's result before child/stop resolution.
    ///
    /// # Returns
    ///
    /// The result with required-child failure or stop state applied.
    #[cfg(test)]
    pub(crate) fn resolve_outcome<T, E>(&self, result: Result<T, E>) -> OperationOutcome<T, E> {
        self.core.resolve_outcome(result)
    }

    /// Reports whether the scope still accepts direct children.
    ///
    /// # Returns
    ///
    /// `true` while child admission remains open.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn is_open(&self) -> bool {
        self.core.is_open()
    }

    #[cfg(test)]
    pub(crate) fn record_test_value(&self, value: usize) {
        self.manager.record_test_value(value);
    }
}

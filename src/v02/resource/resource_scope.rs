// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::TypeId;
use std::sync::Arc;
use std::sync::PoisonError;

use super::ResourceKey;
use super::ResourceRef;
use super::ResourceReservation;
use super::core::ScopeState;
use super::resource_error::ManageResourceError;
use super::resource_error::ManageWithError;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::report::ManagerId;
use crate::v02::report::RecoveryHandle;
use crate::v02::report::ResourceCapacityError;
use crate::v02::report::ResourceLookupError;
use crate::v02::report::ResourceManageErrorKind;
use crate::v02::report::ResourceProgress;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ResourceScopeReport;
#[cfg(test)]
use crate::v02::report::TransferReceipt;
use crate::v02::resource::cleanup_port::CleanupDispatch;

/// Independently endable scope that owns heterogeneous managed resources.
///
/// An operation obtains this scope from its body context with
/// [`crate::OperationScope::resource_scope`].
/// Registered values remain readable through live [`ResourceRef`] handles
/// after the scope ends, while cleanup waits until users release those handles.
/// The public scope handle is not cloneable; the framework retains internal
/// references as needed to finish cleanup and settlement reporting.
///
/// # Examples
///
/// ```
/// use qubit_operation::ResourceScope;
///
/// // An operation body receives its independent resource scope from the
/// // OperationScope context.
/// fn use_resource_scope(scope: &ResourceScope) {
///     match scope.manage(String::from("temporary")) {
///         Ok(resource) => {
///             assert_eq!(&*resource, "temporary");
///             drop(resource);
///         }
///         Err(_error) => return,
///     }
///     scope.end();
/// }
/// ```
#[must_use = "keep the scope alive while managing its resources"]
pub struct ResourceScope {
    /// Shared internal state retained while resources or cleanup remain active.
    state: Arc<ScopeState>,
}
impl ResourceScope {
    /// Creates a scope with manager identity, capacity accounting, and cleanup
    /// dispatch.
    ///
    /// # Parameters
    ///
    /// * `manager` — identity of the manager that owns the scope.
    /// * `id` — unique identity assigned to this resource scope.
    /// * `capacity` — hierarchical resource capacity inherited by this scope.
    /// * `cleanup` — dispatcher that accepts reserved cleanup jobs.
    ///
    /// # Returns
    ///
    /// A new resource scope with open admission and empty ownership state.
    pub(crate) fn new(
        manager: ManagerId,
        id: ResourceScopeId,
        capacity: Arc<ScopeCapacity>,
        cleanup: Arc<dyn CleanupDispatch>,
    ) -> Self {
        Self {
            state: ScopeState::new(manager, id, capacity, cleanup),
        }
    }

    /// Reacquires a resource while this owning scope is still open.
    ///
    /// Lookup validates the key's manager, scope, type, slot, and generation.
    /// The returned reference participates in the same user-reference gate as
    /// the original handle.
    ///
    /// # Parameters
    ///
    /// * `self` — the scope that owns the resource.
    /// * `key` — typed credential returned when the resource was registered.
    ///
    /// # Type Parameters
    ///
    /// `T` is the value type bound into the lookup key.
    ///
    /// # Returns
    ///
    /// A new user reference to the registered value.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceLookupError::ScopeEnded`] after closure,
    /// [`ResourceLookupError::WrongScope`] for a different owner, and
    /// [`ResourceLookupError::StaleKey`] when the key no longer identifies a
    /// matching resource.
    pub fn get_resource<T: Send + Sync + 'static>(
        &self,
        key: ResourceKey<T>,
    ) -> Result<ResourceRef<T>, ResourceLookupError> {
        let core = &self.state.core;
        if key.manager != core.manager || key.scope != core.id {
            return Err(ResourceLookupError::WrongScope);
        }
        if key.type_id != TypeId::of::<T>() {
            return Err(ResourceLookupError::StaleKey);
        }
        let state = core.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.open {
            return Err(ResourceLookupError::ScopeEnded);
        }
        let entry = state.slots.get(&key.slot).ok_or(ResourceLookupError::StaleKey)?;
        let owner = entry
            .owner
            .as_any()
            .downcast_ref::<Arc<super::core::Owner<T>>>()
            .ok_or(ResourceLookupError::StaleKey)?;
        let owner_ref = owner.acquire(key.generation)?;
        let value = owner.value_arc().ok_or(ResourceLookupError::StaleKey)?;
        Ok(ResourceRef {
            owner: owner_ref,
            value: Some(value),
            generation: key.generation,
            manager: core.manager,
            scope: core.id,
            slot: key.slot,
        })
    }
    /// Registers a value for drop-based cleanup and returns a user reference.
    ///
    /// Registration reserves both scope resource capacity and cleanup dispatch
    /// capacity before committing the value. If reservation or commit fails,
    /// the error returns ownership of `value` to the caller.
    ///
    /// # Parameters
    ///
    /// * `self` — this open scope that will own the registered resource.
    /// * `value` — resource whose lifetime the scope will manage.
    ///
    /// # Type Parameters
    ///
    /// `T` must be `Send + Sync + 'static` because the owner and readers may
    /// outlive the call and move between executor threads.
    ///
    /// # Returns
    ///
    /// A [`ResourceRef`] that keeps the resource available to its user while
    /// cleanup remains deferred.
    ///
    /// # Errors
    ///
    /// Returns [`ManageResourceError`] with the original value when the scope
    /// has ended, is stopped, or has exhausted resource or cleanup capacity.
    pub fn manage<T: Send + Sync + 'static>(&self, value: T) -> Result<ResourceRef<T>, ManageResourceError<T>> {
        let reservation = match self.reserve() {
            Ok(reservation) => reservation,
            Err(error) => return Err(ManageResourceError::new(manage_kind(error), value)),
        };
        reservation.commit(value).map_err(|error| {
            let (reason, value, reservation) = error.into_parts();
            drop(reservation);
            ManageResourceError::new(reason, value)
        })
    }

    /// Registers a resource with a caller-defined asynchronous cleanup policy.
    ///
    /// Capacity and cleanup dispatch are reserved before the value and policy
    /// are committed. On failure, the returned error preserves both so the
    /// caller can retry or dispose of them.
    ///
    /// # Parameters
    ///
    /// * `self` — this open scope that will own the registered resource.
    /// * `value` — managed resource whose lifetime the scope will own.
    /// * `policy` — asynchronous action used to release the resource.
    ///
    /// # Type Parameters
    ///
    /// `T` is the shared resource value; `C` is its `AsyncCleanup<T>` policy.
    ///
    /// # Returns
    ///
    /// A [`ResourceRef`] that allows reads while the value remains available.
    ///
    /// # Errors
    ///
    /// Returns [`ManageWithError`] containing the value and policy if the scope
    /// is ended, stopped, or at capacity.
    pub fn manage_with<T, C>(&self, value: T, policy: C) -> Result<ResourceRef<T>, ManageWithError<T, C>>
    where
        T: Send + Sync + 'static,
        C: super::cleanup::AsyncCleanup<T>,
    {
        let reservation = match self.reserve() {
            Ok(reservation) => reservation,
            Err(error) => {
                return Err(ManageWithError::new(manage_kind(error), value, policy));
            }
        };
        reservation.commit_with(value, policy).map_err(|error| {
            let (reason, value, policy, reservation) = error.into_parts();
            drop(reservation);
            ManageWithError::new(reason, value, policy)
        })
    }

    /// Ends admission to this scope and begins cleanup when users release
    /// values.
    ///
    /// Existing [`ResourceRef`] handles remain readable. Cleanup and settlement
    /// callbacks run after outstanding users release their references and the
    /// cleanup dispatcher confirms the resource outcomes.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose admission and resource owners are ended.
    #[inline]
    pub fn end(&self) {
        self.state.end();
    }
    /// Returns the internal identity used to associate reports and resources.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose identity is read.
    ///
    /// # Returns
    ///
    /// The stable identifier assigned to this resource scope.
    #[must_use = "use the scope identifier to associate its resources and reports"]
    #[inline]
    pub(crate) fn id(&self) -> ResourceScopeId {
        self.state.core.id
    }
    /// Counts cleanup tasks currently running for this scope.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose active cleanup tasks are counted.
    ///
    /// # Returns
    ///
    /// The number of cleanup tasks currently in flight.
    #[must_use = "use the cleanup count to observe this scope's progress"]
    #[inline]
    pub(crate) fn cleanup_in_flight(&self) -> usize {
        self.state.cleanup_in_flight()
    }
    /// Reserves one resource slot and cleanup queue entry before resource
    /// creation.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose capacity is reserved.
    ///
    /// # Returns
    ///
    /// A reservation for one resource and its cleanup dispatch entry.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceCapacityError::ScopeEnded`] after closure,
    /// [`ResourceCapacityError::Stopped`] after shutdown, or
    /// [`ResourceCapacityError::CapacityExceeded`] when a capacity limit is
    /// reached.
    #[inline]
    pub(crate) fn reserve(&self) -> Result<ResourceReservation, ResourceCapacityError> {
        self.state.core.reserve()
    }

    /// Retains another internal handle to shared scope state.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose state is shared with the new handle.
    ///
    /// # Returns
    ///
    /// Another internal handle referencing the same scope state.
    #[inline]
    pub(crate) fn internal_clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }

    /// Registers the callback invoked after this scope's resources settle.
    ///
    /// The callback is internal to operation accounting and runs only after
    /// cleanup results are available.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose completed report is observed.
    /// * `callback` — callback invoked with the completed scope report.
    #[inline]
    pub(crate) fn set_settlement_callback(&self, callback: Box<dyn FnOnce(ResourceScopeReport) + Send + 'static>) {
        self.state.set_settlement_callback(callback);
    }

    /// Registers an internal observer for resource cleanup progress changes.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose cleanup progress is observed.
    /// * `callback` — observer notified when cleanup progress changes.
    #[inline]
    pub(crate) fn set_progress_callback(&self, callback: Arc<dyn Fn(ResourceProgress) + Send + Sync + 'static>) {
        self.state.set_progress_callback(callback);
    }

    /// Registers an internal callback for recovery records created by cleanup.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope whose recovery records are observed.
    /// * `callback` — callback receiving each recovery record and its status.
    #[inline]
    pub(crate) fn set_recovery_callback(&self, callback: Arc<dyn Fn(RecoveryHandle, bool) + Send + Sync + 'static>) {
        self.state.set_recovery_callback(callback);
    }

    /// Adds a transfer receipt to the eventual settlement report.
    ///
    /// # Parameters
    ///
    /// * `self` — this scope receiving the transfer evidence.
    /// * `receipt` — accepted transfer to include in the settlement report.
    ///
    /// # Returns
    ///
    /// `true` if the receipt is accepted; `false` when settlement has sealed
    /// the report against further updates.
    #[inline]
    #[cfg(test)]
    pub(crate) fn record_transfer_receipt(&self, receipt: TransferReceipt) -> bool {
        self.state.record_transfer(receipt)
    }
}
impl Drop for ResourceScope {
    /// Ends the scope when this is its final internal handle.
    ///
    /// # Parameters
    ///
    /// * `self` — the final handle being dropped.
    fn drop(&mut self) {
        if Arc::strong_count(&self.state) == 1 {
            self.end();
        }
    }
}

/// Converts an internal capacity failure to the resource-management error kind.
///
/// # Parameters
///
/// * `error` — capacity failure reported by scope admission.
///
/// # Returns
///
/// The corresponding resource-management failure category.
#[must_use = "use the mapped failure category to report or handle the error"]
#[inline]
fn manage_kind(error: ResourceCapacityError) -> ResourceManageErrorKind {
    match error {
        ResourceCapacityError::ScopeEnded => ResourceManageErrorKind::ScopeEnded,
        ResourceCapacityError::Stopped => ResourceManageErrorKind::Stopped,
        ResourceCapacityError::CapacityExceeded => ResourceManageErrorKind::CapacityExceeded,
    }
}

#[cfg(test)]
mod tests {
    use super::manage_kind;
    use crate::v02::report::ResourceCapacityError;
    use crate::v02::report::ResourceManageErrorKind;

    #[test]
    fn test_capacity_failures_map_to_the_matching_manage_error_kind() {
        assert_eq!(
            manage_kind(ResourceCapacityError::ScopeEnded),
            ResourceManageErrorKind::ScopeEnded,
        );
        assert_eq!(
            manage_kind(ResourceCapacityError::Stopped),
            ResourceManageErrorKind::Stopped,
        );
        assert_eq!(
            manage_kind(ResourceCapacityError::CapacityExceeded),
            ResourceManageErrorKind::CapacityExceeded,
        );
    }
}

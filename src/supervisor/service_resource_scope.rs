// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Independent finite resource owner scope associated with a service group.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::ServiceResourceError;
use crate::report::AdmissionError;
use crate::report::ResourceId;

/// Owns a finite resource quota independently from a service group's requests.
///
/// Closing the scope rejects new resource registrations and use leases while
/// allowing existing leases to return. Dropping the scope closes it
/// automatically.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::SupervisorLimits;
/// use qubit_operation::supervisor::OperationSupervisor;
///
/// let supervisor = OperationSupervisor::new(SupervisorLimits::default());
/// let group = supervisor.service_group(1).expect("service group is available");
/// let scope = group.resource_scope(1).expect("resource quota is available");
/// assert!(scope.take_recovery_key().is_none());
/// scope.close();
/// ```
pub struct ServiceResourceScope {
    /// Operation scope that owns registered cleanup resources and recovery
    /// state.
    pub(super) scope: crate::scope::OperationScope,
    /// Shared close flag observed by the service group and this scope.
    pub(super) closed: Arc<std::sync::atomic::AtomicBool>,
}

impl ServiceResourceScope {
    /// Register a cleanup owner while this scope remains open.
    ///
    /// # Type Parameters
    ///
    /// * `C` — cleanup owner transferred to the scope if registration succeeds.
    ///
    /// # Parameters
    ///
    /// * `owner` — value responsible for cleaning up the registered resource.
    ///
    /// # Returns
    ///
    /// Returns the resource identifier on success. On rejection, the error
    /// retains the exact owner so the caller can recover it.
    ///
    /// # Errors
    ///
    /// Returns an error if this scope is closed or its resource quota cannot
    /// accept another owner.
    pub fn register_resource<C: crate::protocol::OwnedCleanup>(
        &self,
        owner: C,
    ) -> Result<ResourceId, ServiceResourceError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ServiceResourceError {
                reason: AdmissionError::Closed,
                owner: Box::new(owner),
            });
        }
        self.scope
            .register_resource(owner)
            .map_err(|error| ServiceResourceError {
                reason: AdmissionError::ReservationLimit,
                owner: error.into_owner(),
            })
    }

    /// Acquire one lease that keeps a registered resource available for use.
    ///
    /// # Parameters
    ///
    /// * `id` — identifier returned when the resource was registered.
    ///
    /// # Returns
    ///
    /// Returns a lease that releases its active-use count when dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if this scope is closed, the identifier is not
    /// registered, or the resource no longer accepts use leases.
    pub fn acquire_use(&self, id: ResourceId) -> Result<crate::resource::ResourceUse, crate::resource::ResourceError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(crate::resource::ResourceError::ClosedForUse);
        }
        self.scope.acquire_use(id)
    }

    /// Close new resource use. Existing leases remain until returned.
    pub fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.scope.close_body_resources();
        }
    }

    /// Take the one-use recovery key for a cleanup owner retained by this
    /// scope.
    ///
    /// # Returns
    ///
    /// Returns `Some` when a recovery key is available, consuming that key
    /// from the scope. Returns `None` when no retained owner has a key.
    pub fn take_recovery_key(&self) -> Option<crate::report::RecoveryKey<crate::report::RecoveryOwner>> {
        self.scope.take_recovery_key()
    }

    /// Recover a typed owner using the key returned by
    /// [`Self::take_recovery_key`].
    ///
    /// # Type Parameters
    ///
    /// * `R` — concrete owner type associated with the recovery key.
    ///
    /// # Parameters
    ///
    /// * `key` — one-use key matching the retained owner and its concrete type.
    ///
    /// # Returns
    ///
    /// Returns the recovered owner on a match, or returns the key unchanged
    /// when it does not identify a recoverable owner of type `R`.
    ///
    /// # Errors
    ///
    /// Returns the original key when it is stale, belongs to another owner,
    /// or identifies an owner with a different concrete type.
    pub fn take_recovery<R: Send + 'static>(
        &self,
        key: crate::report::RecoveryKey<R>,
    ) -> Result<R, crate::report::RecoveryKey<R>> {
        self.scope.take_recovery(key)
    }
}

impl Drop for ServiceResourceScope {
    /// Closes the scope so no new resource use can begin.
    fn drop(&mut self) {
        self.close();
    }
}

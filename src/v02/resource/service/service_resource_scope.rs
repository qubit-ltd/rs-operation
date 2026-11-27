// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::super::ManageResourceError;
use super::super::ResourceCapacityError;
use super::super::ResourceKey;
use super::super::ResourceRef;
use super::super::ResourceReservation;
use super::super::ResourceScope;
use crate::v02::report::ResourceLookupError;

/// A separately bounded resource owner whose close waits for its last user.
///
/// Resources admitted here share their configured capacity independently of
/// resources owned by individual operation scopes. Calling
/// [`close`](Self::close) stops new admissions; outstanding [`ResourceRef`]
/// handles keep values alive until they are dropped and cleanup can complete.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationManager;
/// use qubit_operation::{
///     GroupOptions, ManagerOptions, ServiceResourceOptions,
/// };
///
/// let manager = OperationManager::new(ManagerOptions::default()).unwrap();
/// let group = manager.group(GroupOptions { capacity: 2 }).unwrap();
/// let resources = group
///     .resource_scope(ServiceResourceOptions { capacity: 4 })
///     .unwrap();
/// let value = resources.manage(String::from("shared config")).unwrap();
/// assert_eq!(&*value, "shared config");
/// resources.close();
/// drop(value);
/// ```
#[must_use = "keep the service scope alive while managing resources"]
pub struct ServiceResourceScope {
    /// Underlying resource scope that tracks admission and cleanup.
    resources: ResourceScope,
}

impl ServiceResourceScope {
    /// Wraps an already configured resource scope as a service scope.
    ///
    /// # Parameters
    ///
    /// * `resources` — the scope that owns service resource admission and
    ///   cleanup.
    #[inline]
    pub(crate) fn new(resources: ResourceScope) -> Self {
        Self { resources }
    }

    /// Reserves service resource and cleanup capacity before creating a value.
    ///
    /// # Returns
    ///
    /// A reservation that can be committed with the value after creation.
    /// Dropping the unused reservation returns both permits.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceCapacityError::ScopeEnded`] if the scope is closed,
    /// or [`ResourceCapacityError::CapacityExceeded`] if resource or cleanup
    /// capacity cannot be reserved.
    #[inline]
    pub fn reserve_resource(&self) -> Result<ResourceReservation, ResourceCapacityError> {
        self.resources.reserve_resource()
    }

    /// Admits a value and returns a strong handle to the managed resource.
    ///
    /// The call returns an error with ownership of `resource` when admission
    /// fails.
    ///
    /// # Type Parameters
    ///
    /// * `T` — the resource value type; it must be safe to share across
    ///   threads.
    ///
    /// # Parameters
    ///
    /// * `resource` — the value to register in this service scope.
    ///
    /// # Returns
    ///
    /// A strong handle to the registered resource.
    ///
    /// # Errors
    ///
    /// Returns the original resource with an error if the scope is closed or
    /// its configured capacity is exhausted.
    #[inline]
    pub fn manage<T: Send + Sync + 'static>(&self, resource: T) -> Result<ResourceRef<T>, ManageResourceError<T>> {
        self.resources.manage(resource)
    }

    /// Looks up a resource using a key created from a live resource handle.
    ///
    /// Returns an error if the key belongs to another scope or no longer
    /// identifies a live resource of the requested type.
    ///
    /// # Type Parameters
    ///
    /// * `T` — the resource value type encoded in the lookup key.
    ///
    /// # Parameters
    ///
    /// * `key` — the typed key created from a resource reference.
    ///
    /// # Returns
    ///
    /// A strong handle to the resource identified by `key`.
    ///
    /// # Errors
    ///
    /// Returns an error if the key belongs to another scope, the resource is
    /// no longer live, or its stored type differs from `T`.
    #[inline]
    pub fn get_resource<T: Send + Sync + 'static>(
        &self,
        key: ResourceKey<T>,
    ) -> Result<ResourceRef<T>, ResourceLookupError> {
        self.resources.get_resource(key)
    }

    /// Stops new resource admissions and begins closing this scope.
    ///
    /// Existing handles remain valid and delay cleanup until their final drop.
    ///
    /// # Parameters
    ///
    /// * `self` — this service scope whose admission is being closed.
    #[inline]
    pub fn close(&self) {
        self.resources.end();
    }
}

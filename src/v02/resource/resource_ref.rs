// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::TypeId;
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use super::core::Owner;
use super::resource_key::ResourceKey;
use crate::v02::report::ManagerId;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceScopeId;

/// A strong reference that keeps a resource available until this handle drops.
///
/// Each clone independently delays cleanup. The resource is dereferenced as
/// `T`; when the last handle is dropped after its scope ends, cleanup becomes
/// eligible for background dispatch. Holding a reference does not keep its
/// scope open, but it does keep the resource value alive while that scope
/// settles.
///
/// # Type Parameters
///
/// * `T` — resource value type; it must be `Send + Sync + 'static` so its
///   ownership can cross cleanup worker boundaries.
///
/// # Examples
///
/// A resource reference is returned by a resource scope. Its key can reacquire
/// another strong reference, and clones independently retain the value:
///
/// ```
/// use qubit_operation::{ResourceRef, ResourceScope};
///
/// fn read_managed_value(scope: &ResourceScope) -> ResourceRef<String> {
///     let resource = scope
///         .manage(String::from("payload"))
///         .expect("resource capacity is available");
///     let key = resource.key();
///     let looked_up = scope
///         .get_resource(key)
///         .expect("the resource remains in this scope");
///     assert_eq!(looked_up.id(), resource.id());
///     looked_up.clone()
/// }
/// ```
#[must_use = "use or retain this handle while accessing the resource"]
pub struct ResourceRef<T: Send + Sync + 'static> {
    /// Shared owner whose user count controls when scope cleanup may begin.
    pub(super) owner: Arc<Owner<T>>,
    /// Strong reference to the value dereferenced by this handle.
    pub(super) value: Option<Arc<T>>,
    /// Generation used to ensure owner operations refer to this slot
    /// incarnation.
    pub(super) generation: u64,
    /// Manager that admitted this resource.
    pub(super) manager: ManagerId,
    /// Scope that admitted this resource.
    pub(super) scope: ResourceScopeId,
    /// Manager-local resource slot number.
    pub(super) slot: u64,
}
impl<T: Send + Sync + 'static> Clone for ResourceRef<T> {
    /// Creates another strong handle and increments the owner's live-user
    /// count.
    #[inline]
    fn clone(&self) -> Self {
        let owner = self
            .owner
            .clone_user(self.generation)
            .expect("a live ResourceRef keeps its generation valid");
        Self {
            owner,
            value: self.value.as_ref().map(Arc::clone),
            generation: self.generation,
            manager: self.manager,
            scope: self.scope,
            slot: self.slot,
        }
    }
}
impl<T: Send + Sync + 'static> Deref for ResourceRef<T> {
    /// Value exposed through dereferencing this resource handle.
    type Target = T;

    /// Borrows the resource value retained by this handle.
    ///
    /// # Parameters
    ///
    /// - `self`: Live resource handle being dereferenced.
    ///
    /// # Returns
    ///
    /// A shared reference to the managed value.
    #[inline]
    fn deref(&self) -> &T {
        self.value.as_deref().expect("live ResourceRef owns its resource")
    }
}
impl<T: Send + Sync + 'static> Drop for ResourceRef<T> {
    /// Releases this handle's value reference and owner user count.
    fn drop(&mut self) {
        drop(self.value.take());
        self.owner.release();
    }
}
impl<T: Send + Sync + 'static> ResourceRef<T> {
    /// Returns this resource's stable manager, slot, and generation identity.
    ///
    /// # Returns
    ///
    /// The identity of this resource in its manager's resource table.
    #[must_use]
    #[inline]
    pub fn id(&self) -> ResourceId {
        ResourceId::new(self.manager, self.slot, self.generation)
    }

    /// Creates a typed lookup key for reacquiring this resource in its scope.
    ///
    /// The key does not retain the resource; keep this reference or another
    /// `ResourceRef<T>` alive when the value itself must remain available.
    ///
    /// # Returns
    ///
    /// A key scoped to the manager, scope, and generation stored in this
    /// reference.
    #[must_use]
    #[inline]
    pub fn key(&self) -> ResourceKey<T> {
        ResourceKey {
            manager: self.manager,
            scope: self.scope,
            slot: self.slot,
            generation: self.generation,
            type_id: TypeId::of::<T>(),
            marker: PhantomData,
        }
    }
}

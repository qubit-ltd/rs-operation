// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::TypeId;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::fmt::Result;
use std::marker::PhantomData;

use crate::v02::report::ManagerId;
use crate::v02::report::ResourceScopeId;

/// Identifies a resource of type `T` within one manager and resource scope.
///
/// A key is a copyable lookup token; it does not retain the resource or extend
/// the scope lifetime. Lookup can fail after the scope ends, after a slot is
/// reused with a different generation, or when the stored value has another
/// type. Callers that need to keep a resource alive must retain its
/// [`ResourceRef`](super::ResourceRef) instead.
///
/// # Type Parameters
///
/// * `T` — resource value type, which must be thread-safe and `'static` because
///   resource ownership may be transferred to background cleanup workers.
///
/// # Examples
///
/// A key comes from a live resource reference and can reacquire a reference
/// through the same scope:
///
/// ```
/// use qubit_operation::{ResourceKey, ResourceLookupError, ResourceRef, ResourceScope};
///
/// fn reacquire(
///     scope: &ResourceScope,
///     key: ResourceKey<String>,
/// ) -> Result<ResourceRef<String>, ResourceLookupError> {
///     scope.get_resource(key)
/// }
/// ```
pub struct ResourceKey<T: Send + Sync + 'static> {
    /// Manager that owns the resource slot.
    pub(crate) manager: ManagerId,
    /// Scope that admitted the resource; lookups must use this same scope.
    pub(crate) scope: ResourceScopeId,
    /// Slot number assigned when the resource was committed.
    pub(crate) slot: u64,
    /// Slot generation used to reject keys after slot reuse or scope end.
    pub(crate) generation: u64,
    /// Runtime type identity checked before downcasting the stored value.
    pub(crate) type_id: TypeId,
    /// Associates the key with `T` without owning or borrowing a value of `T`.
    pub(crate) marker: PhantomData<fn() -> T>,
}
impl<T: Send + Sync + 'static> Clone for ResourceKey<T> {
    /// Copies the lookup token without retaining the resource.
    #[inline]
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: Send + Sync + 'static> Copy for ResourceKey<T> {}
impl<T: Send + Sync + 'static> Debug for ResourceKey<T> {
    /// Formats the slot and generation for diagnostics.
    ///
    /// # Parameters
    ///
    /// - `self`: Resource lookup key being formatted.
    /// - `f`: Formatter receiving the diagnostic representation.
    ///
    /// # Returns
    ///
    /// A formatting result indicating whether writing succeeded.
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("ResourceKey")
            .field("slot", &self.slot)
            .field("generation", &self.generation)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::ResourceKey;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ResourceScopeId;

    #[test]
    fn test_key_is_copyable_and_debug_discloses_only_slot_identity() {
        let key = ResourceKey::<String> {
            manager: ManagerId::new_unique(),
            scope: ResourceScopeId::new_unique(),
            slot: 17,
            generation: 4,
            type_id: std::any::TypeId::of::<String>(),
            marker: std::marker::PhantomData,
        };
        let copied = key;
        let cloned = <ResourceKey<String> as Clone>::clone(&key);

        assert_eq!(format!("{key:?}"), "ResourceKey { slot: 17, generation: 4 }");
        assert_eq!(format!("{copied:?}"), format!("{cloned:?}"));
    }
}

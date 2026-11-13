// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::fmt;
use core::marker::PhantomData;

use super::ResourceId;

/// Opaque typed key for taking a resource's recovery owner exactly once.
///
/// It intentionally exposes no `Any` or downcast operation.
/// A key is issued by one recovery registry and is not `Clone`.
///
/// A key is issued by [`crate::resource::RecoveryRegistry::register`] and can
/// be inspected or consumed by
/// [`crate::resource::RecoveryRegistry::take_recovery`].
///
/// # Type Parameters
///
/// `R` is the type of the recovery owner retained by the registry and recovered
/// with this key. It must be `'static` so the owner can be retained
/// independently.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::OperationId;
/// use qubit_operation::report::ResourceId;
/// use qubit_operation::resource::RecoveryRegistry;
///
/// let resource = ResourceId::new(OperationId::new(1), 2, 3);
/// let registry = RecoveryRegistry::new(1);
/// let key = registry
///     .register(resource, String::from("owner"))
///     .expect("the recovery registry has capacity");
/// assert_eq!(key.resource_id(), resource);
/// assert_eq!(registry.take_recovery(key).expect("the key was issued here"), "owner");
/// ```
#[must_use = "use the recovery key to inspect or recover its resource owner"]
pub struct RecoveryKey<R: 'static> {
    /// Identity of the recovery registry that issued this key.
    pub(crate) registry_id: u64,
    /// Resource registration whose recovery owner the key identifies.
    pub(crate) resource: ResourceId,
    /// One-time issuance number used to reject stale or repeated keys.
    pub(crate) nonce: u64,
    /// Associates the key with its resource type without storing a value of
    /// `R`.
    pub(crate) marker: PhantomData<fn() -> R>,
}

impl<R> RecoveryKey<R> {
    /// Bind one recovery registry, resource registration, and issuance nonce
    /// into the opaque key returned by the registry's public registration flow.
    ///
    /// # Parameters
    ///
    /// - `registry_id` identifies the registry that issued the key.
    /// - `resource` is the registration associated with the recovery owner.
    /// - `nonce` distinguishes this issuance from stale or repeated keys.
    ///
    /// # Returns
    ///
    /// An opaque key bound to the supplied registry, resource, and nonce.
    pub(crate) const fn new(registry_id: u64, resource: ResourceId, nonce: u64) -> Self {
        Self {
            registry_id,
            resource,
            nonce,
            marker: PhantomData,
        }
    }

    /// Return the exact resource registration bound to this key.
    ///
    /// # Returns
    ///
    /// The resource identity associated with this recovery key.
    #[must_use]
    #[inline]
    pub const fn resource_id(&self) -> ResourceId {
        self.resource
    }
}

impl<R> fmt::Debug for RecoveryKey<R> {
    /// Formats the key's resource identity without exposing registry internals.
    ///
    /// # Parameters
    ///
    /// - `f` is the formatter that receives the debug representation.
    ///
    /// # Returns
    ///
    /// `Ok(())` when formatting succeeds.
    ///
    /// # Errors
    ///
    /// Returns the formatter's error if writing the debug representation fails.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveryKey")
            .field("resource", &self.resource)
            .finish_non_exhaustive()
    }
}

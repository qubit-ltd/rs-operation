// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

use super::super::RecoveryKey;
use super::super::ResourceId;

/// Opaque owned recovery payload. Taking it requires the original typed key.
/// This object cannot be cloned or inspected through `Any`.
/// A cleanup registry supplies it inside
/// [`super::CleanupOutcome::FailedWithRecovery`].
///
/// # Examples
///
/// ```
/// use qubit_operation::report::RecoveryKey;
/// use qubit_operation::report::RecoveryOwner;
///
/// fn recover(owner: RecoveryOwner, key: RecoveryKey<String>) -> Result<String, RecoveryOwner> {
///     owner.take(key)
/// }
/// ```
///
/// ```compile_fail
/// use qubit_operation::report::RecoveryOwner;
/// use qubit_operation::report::RecoveryKey;
/// let _ = RecoveryOwner::new::<String>(unreachable!(), String::new());
/// ```
pub struct RecoveryOwner {
    /// Namespace of the registry that issued the key.
    pub(crate) registry_id: u64,
    /// Resource generation associated with the recovery payload.
    pub(crate) resource_id: ResourceId,
    /// Nonce that distinguishes keys for the same resource generation.
    pub(crate) nonce: u64,
    /// Type-erased owner, accessible only after all key checks pass.
    pub(crate) value: Box<dyn std::any::Any + Send>,
}

impl RecoveryOwner {
    /// Erase an owner behind the key issued for its resource registration.
    ///
    /// # Type Parameters
    ///
    /// * `R` — owned value type; it must be `Send + 'static` so it can be
    ///   retained without borrowing its creator.
    ///
    /// # Parameters
    ///
    /// * `key` — typed registration key whose namespace, generation, and nonce
    ///   must later match to recover the value.
    /// * `value` — owned recovery payload to retain.
    ///
    /// # Returns
    ///
    /// An opaque owner bound to the key's registry namespace, resource
    /// generation, and nonce.
    pub(crate) fn new<R: Send + 'static>(key: &RecoveryKey<R>, value: R) -> Self {
        Self {
            registry_id: key.registry_id,
            resource_id: key.resource_id(),
            nonce: key.nonce,
            value: Box::new(value),
        }
    }

    /// Return the resource registration whose owner is held here.
    ///
    /// # Returns
    ///
    /// The resource generation associated with this retained owner.
    #[must_use]
    #[inline]
    pub fn resource_id(&self) -> ResourceId {
        self.resource_id
    }

    /// Recover the original owner with its matching typed key.
    ///
    /// On a type, resource-generation, or key mismatch, returns this owner
    /// unchanged. The erased value is never exposed for arbitrary downcasting.
    ///
    /// # Type Parameters
    ///
    /// * `R` — expected owned value type; it must be `Send + 'static`.
    ///
    /// # Parameters
    ///
    /// * `key` — key that must match this owner's registry, resource
    ///   generation, nonce, and original value type.
    ///
    /// # Returns
    ///
    /// * `Ok(value)` — the original owner when every key component matches.
    /// * `Err(owner)` — this owner, unchanged, when any key component differs.
    ///
    /// # Errors
    ///
    /// Returns `Err(self)` if the registry namespace, resource generation,
    /// nonce, or erased owner type does not match the supplied key.
    pub fn take<R: Send + 'static>(self, key: RecoveryKey<R>) -> Result<R, Self> {
        if key.registry_id != self.registry_id || key.resource_id() != self.resource_id || key.nonce != self.nonce {
            return Err(self);
        }
        match self.value.downcast::<R>() {
            Ok(value) => Ok(*value),
            Err(value) => Err(Self {
                registry_id: self.registry_id,
                resource_id: self.resource_id,
                nonce: self.nonce,
                value,
            }),
        }
    }
}

impl fmt::Debug for RecoveryOwner {
    /// Formats the resource identity without exposing the erased payload.
    ///
    /// # Parameters
    ///
    /// * `f` — formatter receiving the debug representation.
    ///
    /// # Returns
    ///
    /// The formatter's success or formatting error status.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecoveryOwner")
            .field("resource_id", &self.resource_id)
            .finish_non_exhaustive()
    }
}

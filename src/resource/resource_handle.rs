// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;
use std::sync::Arc;

use super::DependencyError;
use super::ResourceError;
use super::ResourceUse;
use super::internal::Entry;
use super::internal::EntryPhase;
use super::internal::RegistryInner;
use super::internal::lookup_entry;
use super::internal::reachable;
use crate::report::ResourceId;

/// Identifies a registered resource without exposing its cleanup owner.
///
/// # Examples
///
/// ```
/// use core::future::Future;
/// use core::pin::Pin;
/// use qubit_operation::protocol::CleanupContext;
/// use qubit_operation::protocol::OwnedCleanup;
/// use qubit_operation::report::OperationId;
/// use qubit_operation::report::CleanupOutcome;
/// use qubit_operation::resource::ResourceRegistry;
///
/// struct Cleanup;
///
/// impl OwnedCleanup for Cleanup {
///     fn begin(
///         self: Box<Self>,
///         _context: CleanupContext,
///     ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
///         Box::pin(async { CleanupOutcome::Cleaned })
///     }
/// }
///
/// let registry = ResourceRegistry::new(OperationId::new(1), 1);
/// let handle = registry.register(Box::new(Cleanup)).unwrap();
/// assert_eq!(handle.id().operation, OperationId::new(1));
/// ```
pub struct ResourceHandle {
    /// Keeps the registered entry alive while this handle exists.
    pub(super) entry: Arc<Entry>,
}

impl ResourceHandle {
    /// Return the current ID; successful transfer changes its owning operation
    /// ID.
    ///
    /// # Returns
    ///
    /// The resource's current operation, slot, and generation identity.
    #[must_use]
    pub fn id(&self) -> ResourceId {
        self.entry
            .state
            .lock()
            .map(|state| state.id)
            .unwrap_or_else(|poisoned| poisoned.into_inner().id)
    }

    /// Acquire one non-clone use lease while the resource accepts new use.
    ///
    /// # Returns
    ///
    /// A lease that keeps the cleanup owner alive until the lease is dropped.
    ///
    /// # Errors
    ///
    /// Returns an error if the registration is missing, closed, or its
    /// registry state is unavailable or poisoned.
    pub fn acquire_use(&self) -> Result<ResourceUse, ResourceError> {
        let registry = self.registry()?;
        let registry_state = registry.state.lock().map_err(|_| ResourceError::RegistryPoisoned)?;
        let id = self.id();
        if lookup_entry(&registry_state, id).is_none() {
            return Err(ResourceError::NotRegistered);
        }
        let mut entry_state = self.entry.state.lock().map_err(|_| ResourceError::RegistryPoisoned)?;
        if entry_state.id != id {
            return Err(ResourceError::NotRegistered);
        }
        if entry_state.phase != EntryPhase::Open {
            return Err(ResourceError::ClosedForUse);
        }
        entry_state.active_uses = entry_state
            .active_uses
            .checked_add(1)
            .ok_or(ResourceError::LeaseCountOverflow)?;
        drop(entry_state);
        drop(registry_state);
        Ok(ResourceUse {
            entry: Arc::clone(&self.entry),
            registry,
        })
    }

    /// Prevent future use leases; existing leases keep their cleanup owner
    /// alive.
    ///
    /// # Returns
    ///
    /// `Ok(())` when future leases are disabled.
    ///
    /// # Errors
    ///
    /// Returns an error if the registration is missing, already cleaning, or
    /// its registry state is unavailable or poisoned.
    pub fn close_for_use(&self) -> Result<(), ResourceError> {
        let registry = self.registry()?;
        let registry_state = registry.state.lock().map_err(|_| ResourceError::RegistryPoisoned)?;
        let id = self.id();
        if lookup_entry(&registry_state, id).is_none() {
            return Err(ResourceError::NotRegistered);
        }
        let mut entry_state = self.entry.state.lock().map_err(|_| ResourceError::RegistryPoisoned)?;
        if entry_state.id != id {
            return Err(ResourceError::NotRegistered);
        }
        match entry_state.phase {
            EntryPhase::Open => entry_state.phase = EntryPhase::Closed,
            EntryPhase::Closed => {}
            EntryPhase::Cleaning | EntryPhase::Transferred | EntryPhase::Released => {
                return Err(ResourceError::AlreadyCleaning);
            }
        }
        Ok(())
    }

    /// Make this resource's cleanup wait for `prerequisite` to be confirmed
    /// released.
    ///
    /// # Parameters
    ///
    /// - `prerequisite`: resource whose release must be confirmed first.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the dependency is recorded; cleanup ordering is then
    /// enforced by the registry.
    ///
    /// # Errors
    ///
    /// Returns an error if either handle is unregistered, the handles belong
    /// to different registries, the dependency is self-referential or cyclic,
    /// or the registry lock is poisoned.
    pub fn cleanup_after(&self, prerequisite: &ResourceHandle) -> Result<(), DependencyError> {
        let registry = self.registry().map_err(|_| DependencyError::NotRegistered)?;
        let prerequisite_registry = prerequisite.registry().map_err(|_| DependencyError::NotRegistered)?;
        if !Arc::ptr_eq(&registry, &prerequisite_registry) {
            return Err(DependencyError::DifferentScope);
        }
        let mut state = registry.state.lock().map_err(|_| DependencyError::RegistryPoisoned)?;
        let dependent_id = self.id();
        let prerequisite_id = prerequisite.id();
        if lookup_entry(&state, dependent_id).is_none() || lookup_entry(&state, prerequisite_id).is_none() {
            return Err(DependencyError::NotRegistered);
        }
        if dependent_id == prerequisite_id {
            return Err(DependencyError::SelfDependency);
        }
        if reachable(&state, dependent_id, prerequisite_id) {
            return Err(DependencyError::Cycle);
        }
        state
            .dependencies
            .entry(prerequisite_id)
            .or_default()
            .insert(dependent_id);
        Ok(())
    }

    /// Upgrades the entry's weak registry link while the registry is alive.
    ///
    /// # Returns
    ///
    /// A strong reference to the registry shared by the resource entry.
    ///
    /// # Errors
    ///
    /// Returns an error if the registry lock is poisoned or the registry has
    /// already been dropped.
    pub(super) fn registry(&self) -> Result<Arc<RegistryInner>, ResourceError> {
        self.entry
            .registry
            .lock()
            .map_err(|_| ResourceError::RegistryPoisoned)?
            .upgrade()
            .ok_or(ResourceError::RegistryDropped)
    }
}

impl fmt::Debug for ResourceHandle {
    /// Formats the handle using its current resource identity.
    ///
    /// # Parameters
    ///
    /// - `f`: formatter that receives the debug representation.
    ///
    /// # Returns
    ///
    /// The result of writing the handle's resource identity; the formatter's
    /// error is returned unchanged.
    ///
    /// # Errors
    ///
    /// Returns the formatter's error if writing the debug representation
    /// fails.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceHandle")
            .field("resource_id", &self.id())
            .finish()
    }
}

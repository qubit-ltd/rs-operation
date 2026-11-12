// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use super::DependencyError;
use super::ResourceError;
use super::ResourceUse;
use super::internal::Entry;
use super::internal::EntryPhase;
use super::internal::RegistryInner;
use super::internal::lookup_entry;
use super::internal::reachable;

use crate::report::ResourceId;

use std::fmt;
use std::sync::Arc;

/// A registered resource handle. It does not expose the cleanup owner.
pub struct ResourceHandle {
    pub(super) entry: Arc<Entry>,
}

impl ResourceHandle {
    /// Return the current ID; successful transfer changes its owning operation ID.
    pub fn id(&self) -> ResourceId {
        self.entry
            .state
            .lock()
            .map(|state| state.id)
            .unwrap_or_else(|poisoned| poisoned.into_inner().id)
    }

    pub(super) fn registry(&self) -> Result<Arc<RegistryInner>, ResourceError> {
        self.entry
            .registry
            .lock()
            .map_err(|_| ResourceError::RegistryPoisoned)?
            .upgrade()
            .ok_or(ResourceError::RegistryDropped)
    }

    /// Acquire one non-clone use lease while the resource accepts new use.
    pub fn acquire_use(&self) -> Result<ResourceUse, ResourceError> {
        let registry = self.registry()?;
        let registry_state = registry
            .state
            .lock()
            .map_err(|_| ResourceError::RegistryPoisoned)?;
        let id = self.id();
        if lookup_entry(&registry_state, id).is_none() {
            return Err(ResourceError::NotRegistered);
        }
        let mut entry_state = self
            .entry
            .state
            .lock()
            .map_err(|_| ResourceError::RegistryPoisoned)?;
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
        Ok(ResourceUse {
            entry: Arc::clone(&self.entry),
        })
    }

    /// Prevent future use leases; existing leases keep their cleanup owner alive.
    pub fn close_for_use(&self) -> Result<(), ResourceError> {
        let registry = self.registry()?;
        let registry_state = registry
            .state
            .lock()
            .map_err(|_| ResourceError::RegistryPoisoned)?;
        let id = self.id();
        if lookup_entry(&registry_state, id).is_none() {
            return Err(ResourceError::NotRegistered);
        }
        let mut entry_state = self
            .entry
            .state
            .lock()
            .map_err(|_| ResourceError::RegistryPoisoned)?;
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

    /// Make this resource's cleanup wait for `prerequisite` to be confirmed released.
    pub fn cleanup_after(&self, prerequisite: &ResourceHandle) -> Result<(), DependencyError> {
        let registry = self
            .registry()
            .map_err(|_| DependencyError::NotRegistered)?;
        let prerequisite_registry = prerequisite
            .registry()
            .map_err(|_| DependencyError::NotRegistered)?;
        if !Arc::ptr_eq(&registry, &prerequisite_registry) {
            return Err(DependencyError::DifferentScope);
        }
        let mut state = registry
            .state
            .lock()
            .map_err(|_| DependencyError::RegistryPoisoned)?;
        let dependent_id = self.id();
        let prerequisite_id = prerequisite.id();
        if lookup_entry(&state, dependent_id).is_none()
            || lookup_entry(&state, prerequisite_id).is_none()
        {
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
}

impl fmt::Debug for ResourceHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceHandle")
            .field("resource_id", &self.id())
            .finish()
    }
}

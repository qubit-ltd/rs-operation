// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use super::ReadyCleanup;
use super::RegisterError;
use super::RegisterErrorKind;
use super::RegistryError;
use super::ReleaseReceipt;
use super::ResourceHandle;
use super::TransferError;
use super::internal::Entry;
use super::internal::EntryPhase;
use super::internal::EntryState;
use super::internal::RegistryInner;
use super::internal::RegistryState;
use super::internal::Slot;
use super::internal::connected_group;
use super::internal::free_slot_indices;
use super::internal::has_unreleased_prerequisite;
use super::internal::lookup_entry;
use super::internal::lookup_slot;

use crate::protocol::OwnedCleanup;
use crate::report::OperationId;
use crate::report::ResourceId;
use crate::report::TransferReceipt;

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

static NEXT_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

/// A finite registry scoped to one operation and one supervisor instance.
///
/// Clones share the same bounded registration table. A registry does not run
/// cleanup itself; it only returns cleanup owners whose leases and graph
/// prerequisites are settled.
pub struct ResourceRegistry {
    inner: Arc<RegistryInner>,
}

impl Clone for ResourceRegistry {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl ResourceRegistry {
    /// Create a registry with a fixed maximum number of resource registrations.
    ///
    /// `capacity` may be zero for a scope that accepts no resources. A released
    /// slot can be reused with a higher resource generation.
    pub fn new(operation_id: OperationId, capacity: usize) -> Self {
        let id = NEXT_REGISTRY_ID.fetch_add(1, Ordering::Relaxed);
        let slots = (0..capacity)
            .map(|_| Slot {
                generation: 0,
                exhausted: false,
                entry: None,
            })
            .collect();
        Self {
            inner: Arc::new(RegistryInner {
                id,
                operation_id,
                state: Mutex::new(RegistryState {
                    slots,
                    dependencies: BTreeMap::new(),
                }),
            }),
        }
    }

    /// Register an owned cleanup adapter, returning it unchanged if capacity is full.
    pub fn register(&self, owner: Box<dyn OwnedCleanup>) -> Result<ResourceHandle, RegisterError> {
        self.register_with_phase(owner, EntryPhase::Open)
    }

    /// Accept a cleanup owner from an adapter and issue proof of acceptance.
    ///
    /// The returned receipt identifies the source generation. The accepted
    /// owner becomes a closed registration and can be taken by this registry's
    /// cleanup driver. If capacity is unavailable, the error returns `owner`
    /// unchanged and no receipt is produced.
    pub fn accept_cleanup_transfer(
        &self,
        source_resource: ResourceId,
        owner: Box<dyn OwnedCleanup>,
    ) -> Result<(ResourceId, TransferReceipt), RegisterError> {
        let accepted = self.register_with_phase(owner, EntryPhase::Closed)?;
        let accepted_id = accepted.id();
        if let Ok(mut accepted_state) = accepted.entry.state.lock() {
            accepted_state.transfer_source = Some(source_resource);
        }
        let receipt = TransferReceipt::accepted_by_registry(source_resource, accepted_id);
        Ok((accepted_id, receipt))
    }

    fn register_with_phase(
        &self,
        owner: Box<dyn OwnedCleanup>,
        phase: EntryPhase,
    ) -> Result<ResourceHandle, RegisterError> {
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(_) => {
                return Err(RegisterError {
                    kind: RegisterErrorKind::RegistryPoisoned,
                    owner: Some(owner),
                });
            }
        };
        let Some((slot_index, slot)) = state
            .slots
            .iter_mut()
            .enumerate()
            .find(|(_, slot)| !slot.exhausted && slot.entry.is_none())
        else {
            return Err(RegisterError {
                kind: RegisterErrorKind::CapacityExceeded,
                owner: Some(owner),
            });
        };
        let resource_id = ResourceId::new(
            self.inner.operation_id,
            u32::try_from(slot_index).unwrap_or(u32::MAX),
            slot.generation,
        );
        let entry = Arc::new(Entry {
            registry: Mutex::new(Arc::downgrade(&self.inner)),
            state: Mutex::new(EntryState {
                id: resource_id,
                phase,
                active_uses: 0,
                transfer_source: None,
                transfer_receiver: None,
            }),
            owner: Mutex::new(Some(owner)),
        });
        slot.entry = Some(Arc::clone(&entry));
        Ok(ResourceHandle { entry })
    }

    /// Return whether this exact resource generation is still registered here.
    pub fn contains(&self, resource_id: ResourceId) -> bool {
        let Ok(state) = self.inner.state.lock() else {
            return false;
        };
        lookup_entry(&state, resource_id).is_some()
    }

    /// Count registrations whose local cleanup responsibility is still pending.
    /// A transferred prerequisite remains pending while it blocks dependent cleanup.
    pub fn pending_cleanup_count(&self) -> usize {
        let Ok(state) = self.inner.state.lock() else {
            return usize::MAX;
        };
        state
            .slots
            .iter()
            .filter_map(|slot| slot.entry.as_ref())
            .filter(|entry| {
                let Ok(entry_state) = entry.state.lock() else {
                    return true;
                };
                match entry_state.phase {
                    EntryPhase::Released => false,
                    EntryPhase::Transferred => state
                        .dependencies
                        .get(&entry_state.id)
                        .is_some_and(|dependents| !dependents.is_empty()),
                    EntryPhase::Open | EntryPhase::Closed | EntryPhase::Cleaning => true,
                }
            })
            .count()
    }

    /// Whether a closed, unused registration can be handed to a cleanup driver now.
    pub fn has_ready_cleanup(&self) -> bool {
        let Ok(state) = self.inner.state.lock() else {
            return false;
        };
        state
            .slots
            .iter()
            .filter_map(|slot| slot.entry.as_ref())
            .any(|entry| {
                let Ok(entry_state) = entry.state.lock() else {
                    return false;
                };
                entry_state.phase == EntryPhase::Closed
                    && entry_state.active_uses == 0
                    && !has_unreleased_prerequisite(&state, entry_state.id)
            })
    }

    /// Take the first resource whose use leases and cleanup prerequisites are settled.
    ///
    /// Resources are considered in ascending resource ID order. The returned
    /// owner must be driven outside registry locks and followed by
    /// [`Self::confirm_released`] only after release is actually confirmed.
    pub fn take_ready_cleanup(&self) -> Option<ReadyCleanup> {
        let state = self.inner.state.lock().ok()?;
        for slot in &state.slots {
            let Some(entry) = &slot.entry else {
                continue;
            };
            let Ok(mut entry_state) = entry.state.lock() else {
                continue;
            };
            if entry_state.phase != EntryPhase::Closed
                || entry_state.active_uses != 0
                || has_unreleased_prerequisite(&state, entry_state.id)
            {
                continue;
            }
            let mut owner_slot = entry.owner.lock().ok()?;
            let owner = owner_slot.take()?;
            entry_state.phase = EntryPhase::Cleaning;
            return Some(ReadyCleanup {
                resource_id: entry_state.id,
                owner: Some(owner),
            });
        }
        None
    }

    /// Record confirmed release and make the registry slot reusable.
    pub fn confirm_released(&self, resource_id: ResourceId) -> Result<(), RegistryError> {
        self.finish_release(resource_id, None, false).map(|_| ())
    }

    /// Confirm receiver-side release and issue proof for the source tombstone.
    pub fn confirm_released_with_receipt(
        &self,
        resource_id: ResourceId,
    ) -> Result<Option<ReleaseReceipt>, RegistryError> {
        self.finish_release(resource_id, None, true)
    }

    /// Clear a transferred source tombstone using the receiver's release proof.
    pub fn confirm_transferred_released(
        &self,
        resource_id: ResourceId,
        receipt: ReleaseReceipt,
    ) -> Result<(), RegistryError> {
        if receipt.source() != resource_id {
            return Err(RegistryError::InvalidReceipt);
        }
        self.finish_release(resource_id, Some(receipt), false)
            .map(|_| ())
    }

    fn finish_release(
        &self,
        resource_id: ResourceId,
        receipt: Option<ReleaseReceipt>,
        issue_release_receipt: bool,
    ) -> Result<Option<ReleaseReceipt>, RegistryError> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| RegistryError::RegistryPoisoned)?;
        let (slot_index, entry) =
            lookup_slot(&state, resource_id).ok_or(RegistryError::NotRegistered)?;
        let entry = Arc::clone(entry);
        {
            let mut entry_state = entry
                .state
                .lock()
                .map_err(|_| RegistryError::RegistryPoisoned)?;
            if entry_state.phase == EntryPhase::Cleaning
                && entry_state.transfer_source.is_some()
                && receipt.is_none()
                && !issue_release_receipt
            {
                return Err(RegistryError::InvalidReceipt);
            }
            if entry_state.phase == EntryPhase::Transferred {
                let Some(receipt) = receipt else {
                    return Err(RegistryError::NotCleaning);
                };
                if receipt.source() != resource_id
                    || Some(receipt.receiver()) != entry_state.transfer_receiver
                {
                    return Err(RegistryError::InvalidReceipt);
                }
            } else if entry_state.phase != EntryPhase::Cleaning {
                return Err(RegistryError::NotCleaning);
            }
            let release_receipt = entry_state
                .transfer_source
                .map(|source| ReleaseReceipt::confirmed(source, resource_id));
            entry_state.phase = EntryPhase::Released;
            state.dependencies.remove(&resource_id);
            for dependents in state.dependencies.values_mut() {
                dependents.remove(&resource_id);
            }
            release_slot(&mut state.slots[slot_index]);
            Ok(release_receipt)
        }
    }

    /// Confirm transfer only after a receiver-issued receipt matches this generation.
    ///
    /// A transferred resource remains registered until its receiver confirms
    /// release. Its cleanup dependents stay blocked during that interval.
    pub fn confirm_transferred(
        &self,
        resource_id: ResourceId,
        receipt: TransferReceipt,
    ) -> Result<(), RegistryError> {
        if receipt.resource() != resource_id || receipt.generation() != resource_id.generation {
            return Err(RegistryError::InvalidReceipt);
        }
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| RegistryError::RegistryPoisoned)?;
        let (_, entry) = lookup_slot(&state, resource_id).ok_or(RegistryError::NotRegistered)?;
        let entry = Arc::clone(entry);
        {
            let mut entry_state = entry
                .state
                .lock()
                .map_err(|_| RegistryError::RegistryPoisoned)?;
            if entry_state.phase != EntryPhase::Cleaning {
                return Err(RegistryError::NotCleaning);
            }
            entry_state.transfer_receiver = Some(receipt.receiver());
            entry_state.phase = EntryPhase::Transferred;
        }
        Ok(())
    }

    /// Record that the supervisor's recovery registry accepted an owned cleanup failure.
    pub fn accept_recovery(&self, resource_id: ResourceId) -> Result<(), RegistryError> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| RegistryError::RegistryPoisoned)?;
        let (_, entry) = lookup_slot(&state, resource_id).ok_or(RegistryError::NotRegistered)?;
        let mut entry_state = entry
            .state
            .lock()
            .map_err(|_| RegistryError::RegistryPoisoned)?;
        if entry_state.phase != EntryPhase::Cleaning {
            return Err(RegistryError::NotCleaning);
        }
        entry_state.phase = EntryPhase::Transferred;
        Ok(())
    }

    /// Restore an unaccepted cleanup owner after a receiver rejects transfer.
    ///
    /// On failure, the tuple contains the original owner so its caller can retain
    /// or route responsibility elsewhere. A successful restore makes cleanup
    /// ready for a later attempt.
    pub fn restore_cleanup_owner(
        &self,
        resource_id: ResourceId,
        owner: Box<dyn OwnedCleanup>,
    ) -> Result<(), (RegistryError, Box<dyn OwnedCleanup>)> {
        let state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(_) => return Err((RegistryError::RegistryPoisoned, owner)),
        };
        let Some(entry) = lookup_entry(&state, resource_id).cloned() else {
            return Err((RegistryError::NotRegistered, owner));
        };
        let mut entry_state = match entry.state.lock() {
            Ok(entry_state) => entry_state,
            Err(_) => return Err((RegistryError::RegistryPoisoned, owner)),
        };
        if entry_state.phase != EntryPhase::Cleaning {
            return Err((RegistryError::NotCleaning, owner));
        }
        let mut owner_slot = match entry.owner.lock() {
            Ok(owner_slot) => owner_slot,
            Err(_) => return Err((RegistryError::RegistryPoisoned, owner)),
        };
        if owner_slot.is_some() {
            return Err((RegistryError::OwnerUnavailable, owner));
        }
        *owner_slot = Some(owner);
        entry_state.phase = EntryPhase::Closed;
        Ok(())
    }

    /// Move the resource's entire connected dependency group into `destination`.
    ///
    /// Capacity is reserved for the whole group before either registry changes.
    /// Existing handles and use leases follow the moved entries. If the group
    /// does not fit, all registrations and owner responsibility remain here.
    pub fn transfer_to(
        &self,
        resource: &ResourceHandle,
        destination: &ResourceRegistry,
    ) -> Result<(), TransferError> {
        if Arc::ptr_eq(&self.inner, &destination.inner) {
            return Err(TransferError::SameRegistry);
        }
        let first_id = resource.id();
        let resource_registry = resource
            .registry()
            .map_err(|_| TransferError::NotRegistered)?;
        if !Arc::ptr_eq(&resource_registry, &self.inner) {
            return Err(TransferError::NotRegistered);
        }
        let (mut source_state, mut destination_state) = lock_registries(self, destination)?;
        if lookup_entry(&source_state, first_id).is_none() {
            return Err(TransferError::NotRegistered);
        }

        let group = connected_group(&source_state, first_id);
        let target_slots = free_slot_indices(&destination_state, group.len());
        if target_slots.len() != group.len() {
            return Err(TransferError::CapacityExceeded);
        }
        let mut remapped = BTreeMap::new();
        let mut moved_entries = Vec::with_capacity(group.len());
        for (old_id, target_slot_index) in group.iter().zip(target_slots) {
            let (source_slot_index, entry) =
                lookup_slot(&source_state, *old_id).ok_or(TransferError::NotRegistered)?;
            let target_slot = &destination_state.slots[target_slot_index];
            let new_id = ResourceId::new(
                destination.inner.operation_id,
                u32::try_from(target_slot_index).unwrap_or(u32::MAX),
                target_slot.generation,
            );
            remapped.insert(*old_id, new_id);
            moved_entries.push((
                source_slot_index,
                target_slot_index,
                Arc::clone(entry),
                new_id,
            ));
        }

        let mut moved_edges = Vec::new();
        for (prerequisite, dependents) in &source_state.dependencies {
            for dependent in dependents {
                if group.binary_search(prerequisite).is_ok() {
                    moved_edges.push((remapped[prerequisite], remapped[dependent]));
                }
            }
        }

        for (source_slot, target_slot, entry, new_id) in &moved_entries {
            {
                let mut entry_state = entry
                    .state
                    .lock()
                    .map_err(|_| TransferError::RegistryPoisoned)?;
                entry_state.id = *new_id;
            }
            *entry
                .registry
                .lock()
                .map_err(|_| TransferError::RegistryPoisoned)? = Arc::downgrade(&destination.inner);
            release_slot(&mut source_state.slots[*source_slot]);
            destination_state.slots[*target_slot].entry = Some(Arc::clone(entry));
        }
        source_state
            .dependencies
            .retain(|prerequisite, dependents| {
                dependents.retain(|dependent| group.binary_search(dependent).is_err());
                group.binary_search(prerequisite).is_err() && !dependents.is_empty()
            });
        for (prerequisite, dependent) in moved_edges {
            destination_state
                .dependencies
                .entry(prerequisite)
                .or_default()
                .insert(dependent);
        }
        Ok(())
    }
}

fn release_slot(slot: &mut Slot) {
    slot.entry = None;
    if let Some(next_generation) = slot.generation.checked_add(1) {
        slot.generation = next_generation;
    } else {
        slot.exhausted = true;
    }
}

fn lock_registries<'a>(
    source: &'a ResourceRegistry,
    destination: &'a ResourceRegistry,
) -> Result<
    (
        std::sync::MutexGuard<'a, RegistryState>,
        std::sync::MutexGuard<'a, RegistryState>,
    ),
    TransferError,
> {
    if source.inner.id < destination.inner.id {
        let source_state = source
            .inner
            .state
            .lock()
            .map_err(|_| TransferError::RegistryPoisoned)?;
        let destination_state = destination
            .inner
            .state
            .lock()
            .map_err(|_| TransferError::RegistryPoisoned)?;
        Ok((source_state, destination_state))
    } else {
        let destination_state = destination
            .inner
            .state
            .lock()
            .map_err(|_| TransferError::RegistryPoisoned)?;
        let source_state = source
            .inner
            .state
            .lock()
            .map_err(|_| TransferError::RegistryPoisoned)?;
        Ok((source_state, destination_state))
    }
}

impl fmt::Debug for ResourceRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceRegistry")
            .field("operation_id", &self.inner.operation_id)
            .field(
                "capacity",
                &self.inner.state.lock().map(|state| state.slots.len()).ok(),
            )
            .finish()
    }
}

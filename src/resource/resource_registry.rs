// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

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

static NEXT_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

/// A finite registry scoped to one operation and one supervisor instance.
///
/// Clones share the same bounded registration table. A registry does not run
/// cleanup itself; it only returns cleanup owners whose leases and graph
/// prerequisites are settled.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::ResourceRegistry;
/// use qubit_operation::report::OperationId;
///
/// let registry = ResourceRegistry::new(OperationId::new(7), 4);
/// assert!(!registry.has_ready_cleanup());
/// assert_eq!(registry.pending_cleanup_count(), 0);
/// ```
pub struct ResourceRegistry {
    /// Shared registry state used by every clone of this handle.
    inner: Arc<RegistryInner>,
}

impl Clone for ResourceRegistry {
    /// Creates another handle to the same registration table.
    ///
    /// # Returns
    ///
    /// A clone that shares this registry's state.
    #[inline]
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
    ///
    /// # Parameters
    ///
    /// * `operation_id` — Operation that owns registrations in this registry.
    /// * `capacity` — Maximum number of simultaneously registered resources.
    ///
    /// # Returns
    ///
    /// A registry with the requested fixed capacity.
    #[must_use]
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
                change_epoch: AtomicU64::new(0),
                change_wakers: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Return whether this exact resource generation is still registered here.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — Exact operation, slot, and generation to look up.
    ///
    /// # Returns
    ///
    /// `true` if that exact generation is registered; otherwise `false`.
    #[must_use]
    pub fn contains(&self, resource_id: ResourceId) -> bool {
        let Ok(state) = self.inner.state.lock() else {
            return false;
        };
        lookup_entry(&state, resource_id).is_some()
    }

    /// Count registrations whose local cleanup responsibility is still pending.
    /// A transferred prerequisite remains pending while it blocks dependent
    /// cleanup.
    ///
    /// # Returns
    ///
    /// The number of registrations still carrying cleanup responsibility. A
    /// poisoned registry returns `usize::MAX` as a conservative sentinel.
    #[must_use]
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

    /// Whether a closed, unused registration can be handed to a cleanup driver
    /// now.
    ///
    /// # Returns
    ///
    /// `true` when at least one registration is ready; otherwise `false`.
    #[must_use]
    pub fn has_ready_cleanup(&self) -> bool {
        let Ok(state) = self.inner.state.lock() else {
            return false;
        };
        state.slots.iter().filter_map(|slot| slot.entry.as_ref()).any(|entry| {
            let Ok(entry_state) = entry.state.lock() else {
                return false;
            };
            entry_state.phase == EntryPhase::Closed
                && entry_state.active_uses == 0
                && !has_unreleased_prerequisite(&state, entry_state.id)
        })
    }

    /// Register an owned cleanup adapter, returning it unchanged if capacity is
    /// full.
    ///
    /// # Parameters
    ///
    /// * `owner` — Owned cleanup adapter retained until cleanup is taken or
    ///   returned in an error.
    ///
    /// # Returns
    ///
    /// A handle to the new registration.
    ///
    /// # Errors
    ///
    /// Returns the owner unchanged if the registry is poisoned or at capacity.
    pub fn register(&self, owner: Box<dyn OwnedCleanup>) -> Result<ResourceHandle, RegisterError> {
        self.register_with_phase(owner, EntryPhase::Open)
    }

    /// Accept a cleanup owner from an adapter and issue proof of acceptance.
    ///
    /// The returned receipt identifies the source generation. The accepted
    /// owner becomes a closed registration and can be taken by this registry's
    /// cleanup driver. If capacity is unavailable, the error returns `owner`
    /// unchanged and no receipt is produced.
    ///
    /// # Parameters
    ///
    /// * `source_resource` — Source generation whose ownership is transferred.
    /// * `owner` — Cleanup owner accepted as a closed registration.
    ///
    /// # Returns
    ///
    /// The accepted resource ID and receiver-verifiable receipt linking it to
    /// `source_resource`.
    ///
    /// # Errors
    ///
    /// Returns the owner unchanged if registration fails.
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

    /// Take the first resource whose use leases and cleanup prerequisites are
    /// settled.
    ///
    /// Resources are considered in ascending resource ID order. The returned
    /// owner must be driven outside registry locks and followed by
    /// [`Self::confirm_released`] only after release is actually confirmed.
    ///
    /// # Returns
    ///
    /// `Some` contains one ready owner; `None` means none is ready or registry
    /// state could not be locked.
    #[must_use = "a ready cleanup owner must be driven or returned"]
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
    ///
    /// # Parameters
    ///
    /// * `resource_id` — Resource whose release has been externally confirmed.
    ///
    /// # Returns
    ///
    /// `()` after the release has been recorded.
    ///
    /// # Errors
    ///
    /// Returns an error if the resource is unknown, not cleaning, or requires
    /// receiver release proof.
    pub fn confirm_released(&self, resource_id: ResourceId) -> Result<(), RegistryError> {
        self.finish_release(resource_id, None, false).map(|_| ())
    }

    /// Confirm receiver-side release and issue proof for the source tombstone.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — Resource whose receiver-side release was confirmed.
    ///
    /// # Returns
    ///
    /// `Ok(Some(receipt))` when a source tombstone needs acknowledgement;
    /// `Ok(None)` when no source receipt is needed.
    ///
    /// # Errors
    ///
    /// Returns an error if the resource is unknown or not in a releasable
    /// phase.
    pub fn confirm_released_with_receipt(
        &self,
        resource_id: ResourceId,
    ) -> Result<Option<ReleaseReceipt>, RegistryError> {
        self.finish_release(resource_id, None, true)
    }

    /// Clear a transferred source tombstone using the receiver's release proof.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — Transferred source generation to clear.
    /// * `receipt` — Receiver proof matching that source generation.
    ///
    /// # Returns
    ///
    /// `()` after the transferred source has been cleared.
    ///
    /// # Errors
    ///
    /// Returns an error if the receipt does not match or the source is unknown.
    pub fn confirm_transferred_released(
        &self,
        resource_id: ResourceId,
        receipt: ReleaseReceipt,
    ) -> Result<(), RegistryError> {
        if receipt.source() != resource_id {
            return Err(RegistryError::InvalidReceipt);
        }
        self.finish_release(resource_id, Some(receipt), false).map(|_| ())
    }

    /// Confirm transfer only after a receiver-issued receipt matches this
    /// generation.
    ///
    /// A transferred resource remains registered until its receiver confirms
    /// release. Its cleanup dependents stay blocked during that interval.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — Resource whose ownership moved to a receiver.
    /// * `receipt` — Acceptance proof issued by that receiver.
    ///
    /// # Returns
    ///
    /// `()` after the transfer has been recorded.
    ///
    /// # Errors
    ///
    /// Returns an error if the receipt is invalid, the resource is unknown, or
    /// cleanup is not in progress.
    pub fn confirm_transferred(&self, resource_id: ResourceId, receipt: TransferReceipt) -> Result<(), RegistryError> {
        if receipt.resource() != resource_id || receipt.generation() != resource_id.generation {
            return Err(RegistryError::InvalidReceipt);
        }
        let state = self.inner.state.lock().map_err(|_| RegistryError::RegistryPoisoned)?;
        let (_, entry) = lookup_slot(&state, resource_id).ok_or(RegistryError::NotRegistered)?;
        let entry = Arc::clone(entry);
        {
            let mut entry_state = entry.state.lock().map_err(|_| RegistryError::RegistryPoisoned)?;
            if entry_state.phase != EntryPhase::Cleaning {
                return Err(RegistryError::NotCleaning);
            }
            entry_state.transfer_receiver = Some(receipt.receiver());
            entry_state.phase = EntryPhase::Transferred;
        }
        Ok(())
    }

    /// Record that the supervisor's recovery registry accepted an owned cleanup
    /// failure.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — Cleaning resource now owned by recovery.
    ///
    /// # Returns
    ///
    /// `()` after recovery responsibility has been recorded.
    ///
    /// # Errors
    ///
    /// Returns an error if the resource is unknown or cleanup is not in
    /// progress.
    pub fn accept_recovery(&self, resource_id: ResourceId) -> Result<(), RegistryError> {
        let state = self.inner.state.lock().map_err(|_| RegistryError::RegistryPoisoned)?;
        let (_, entry) = lookup_slot(&state, resource_id).ok_or(RegistryError::NotRegistered)?;
        let mut entry_state = entry.state.lock().map_err(|_| RegistryError::RegistryPoisoned)?;
        if entry_state.phase != EntryPhase::Cleaning {
            return Err(RegistryError::NotCleaning);
        }
        entry_state.phase = EntryPhase::Transferred;
        Ok(())
    }

    /// Restore an unaccepted cleanup owner after a receiver rejects transfer.
    ///
    /// On failure, the tuple contains the original owner so its caller can
    /// retain or route responsibility elsewhere. A successful restore makes
    /// cleanup ready for a later attempt.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — Resource whose cleanup owner is being restored.
    /// * `owner` — Original cleanup adapter returned by the rejecting receiver.
    ///
    /// # Returns
    ///
    /// `()` after the owner is restored and cleanup is ready for another
    /// attempt.
    ///
    /// # Errors
    ///
    /// Returns the original owner if the resource cannot accept it.
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

    /// Move the resource's entire connected dependency group into
    /// `destination`.
    ///
    /// Capacity is reserved for the whole group before either registry changes.
    /// Existing handles and use leases follow the moved entries. If the group
    /// does not fit, all registrations and owner responsibility remain here.
    ///
    /// # Parameters
    ///
    /// * `resource` — Handle identifying the connected group to move.
    /// * `destination` — Registry that receives the whole group.
    ///
    /// # Returns
    ///
    /// `()` after all registrations and internal dependency edges move.
    ///
    /// # Errors
    ///
    /// Returns an error if either registry is poisoned, the resource is not
    /// registered here, the registries are identical, or the destination lacks
    /// capacity.
    pub fn transfer_to(&self, resource: &ResourceHandle, destination: &ResourceRegistry) -> Result<(), TransferError> {
        if Arc::ptr_eq(&self.inner, &destination.inner) {
            return Err(TransferError::SameRegistry);
        }
        let first_id = resource.id();
        let resource_registry = resource.registry().map_err(|_| TransferError::NotRegistered)?;
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
            let (source_slot_index, entry) = lookup_slot(&source_state, *old_id).ok_or(TransferError::NotRegistered)?;
            let target_slot = &destination_state.slots[target_slot_index];
            let new_id = ResourceId::new(
                destination.inner.operation_id,
                u32::try_from(target_slot_index).unwrap_or(u32::MAX),
                target_slot.generation,
            );
            remapped.insert(*old_id, new_id);
            moved_entries.push((source_slot_index, target_slot_index, Arc::clone(entry), new_id));
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
                let mut entry_state = entry.state.lock().map_err(|_| TransferError::RegistryPoisoned)?;
                entry_state.id = *new_id;
            }
            *entry.registry.lock().map_err(|_| TransferError::RegistryPoisoned)? = Arc::downgrade(&destination.inner);
            release_slot(&mut source_state.slots[*source_slot]);
            destination_state.slots[*target_slot].entry = Some(Arc::clone(entry));
        }
        source_state.dependencies.retain(|prerequisite, dependents| {
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

    /// Returns the change counter used by internal cleanup waiters.
    ///
    /// # Returns
    ///
    /// The current registry change counter.
    #[must_use]
    #[inline]
    pub(crate) fn change_epoch(&self) -> u64 {
        self.inner.change_epoch()
    }

    /// Wakes internal cleanup waiters after registry state changes.
    #[inline]
    pub(crate) fn notify_change(&self) {
        self.inner.notify_change();
    }

    /// Registers an internal waiter for subsequent registry changes.
    ///
    /// # Parameters
    ///
    /// * `waker` — Waiter to notify after a subsequent registry change.
    #[inline]
    pub(crate) fn register_change_waker(&self, waker: &std::task::Waker) {
        self.inner.register_change_waker(waker);
    }

    /// Removes an internal waiter when it no longer needs registry changes.
    ///
    /// # Parameters
    ///
    /// * `waker` — Waiter to remove from the registry's notification list.
    #[inline]
    pub(crate) fn remove_change_waker(&self, waker: &std::task::Waker) {
        self.inner.remove_change_waker(waker);
    }

    /// Inserts an owner using the supplied initial lifecycle phase.
    ///
    /// # Parameters
    ///
    /// * `owner` — Cleanup owner to retain in the registry.
    /// * `phase` — Initial lifecycle phase for the registration.
    ///
    /// # Returns
    ///
    /// A handle to the inserted resource.
    ///
    /// # Errors
    ///
    /// Returns the owner if the registry is poisoned or has no reusable slot.
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

    /// Applies a confirmed release and optionally creates a source receipt.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — Registered resource whose release was confirmed.
    /// * `receipt` — Optional receiver proof for a transferred resource.
    /// * `issue_release_receipt` — Whether to issue proof for a source
    ///   tombstone.
    ///
    /// # Returns
    ///
    /// * `Some` — Proof linking a released receiver registration to its source.
    /// * `None` — No transferred source requires a release acknowledgement.
    ///
    /// # Errors
    ///
    /// Returns an error if the resource state or supplied proof is invalid.
    fn finish_release(
        &self,
        resource_id: ResourceId,
        receipt: Option<ReleaseReceipt>,
        issue_release_receipt: bool,
    ) -> Result<Option<ReleaseReceipt>, RegistryError> {
        let mut state = self.inner.state.lock().map_err(|_| RegistryError::RegistryPoisoned)?;
        let (slot_index, entry) = lookup_slot(&state, resource_id).ok_or(RegistryError::NotRegistered)?;
        let entry = Arc::clone(entry);
        {
            let mut entry_state = entry.state.lock().map_err(|_| RegistryError::RegistryPoisoned)?;
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
                if receipt.source() != resource_id || Some(receipt.receiver()) != entry_state.transfer_receiver {
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
}

/// Clears a slot and advances its generation, exhausting it on overflow.
///
/// # Parameters
///
/// * `slot` — Slot to clear and advance.
fn release_slot(slot: &mut Slot) {
    slot.entry = None;
    if let Some(next_generation) = slot.generation.checked_add(1) {
        slot.generation = next_generation;
    } else {
        slot.exhausted = true;
    }
}

/// Locks both registries in stable ID order to prevent transfer deadlocks.
///
/// # Parameters
///
/// * `source` — Registry relinquishing the transferred resources.
/// * `destination` — Registry receiving the transferred resources.
///
/// # Returns
///
/// State guards in source, destination order.
///
/// # Errors
///
/// Returns an error if either registry mutex is poisoned.
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
        let source_state = source.inner.state.lock().map_err(|_| TransferError::RegistryPoisoned)?;
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
        let source_state = source.inner.state.lock().map_err(|_| TransferError::RegistryPoisoned)?;
        Ok((source_state, destination_state))
    }
}

impl fmt::Debug for ResourceRegistry {
    /// Formats the operation identity and current capacity without exposing
    /// owners.
    ///
    /// # Parameters
    ///
    /// * `f` — Formatter receiving the debug representation.
    ///
    /// # Returns
    ///
    /// `Ok(())` after formatting succeeds.
    ///
    /// # Errors
    ///
    /// Returns the formatter's error if writing the representation fails.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourceRegistry")
            .field("operation_id", &self.inner.operation_id)
            .field("capacity", &self.inner.state.lock().map(|state| state.slots.len()).ok())
            .finish()
    }
}

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;

use super::Entry;
use super::RegistryState;
use crate::report::ResourceId;

/// Finds the live registry entry matching a resource's slot and generation.
///
/// # Parameters
///
/// - `state`: Registry slots to inspect.
/// - `resource_id`: Slot and generation identity to match.
///
/// # Returns
///
/// `Some` contains the slot index and entry when both the slot generation and
/// locked entry identity match; `None` indicates a missing, stale, or poisoned
/// entry.
pub(in crate::resource) fn lookup_slot(state: &RegistryState, resource_id: ResourceId) -> Option<(usize, &Arc<Entry>)> {
    let slot_index = resource_id.slot as usize;
    let slot = state.slots.get(slot_index)?;
    if slot.generation != resource_id.generation {
        return None;
    }
    let entry = slot.entry.as_ref()?;
    let entry_state = entry.state.lock().ok()?;
    (entry_state.id == resource_id).then_some((slot_index, entry))
}

/// Looks up a live entry by resource identity without returning its slot index.
///
/// # Parameters
///
/// - `state`: Registry slots to inspect.
/// - `resource_id`: Slot and generation identity to match.
///
/// # Returns
///
/// `Some` borrows the matching entry; `None` indicates that the identity is
/// absent, stale, or backed by a poisoned entry lock.
#[inline]
pub(in crate::resource) fn lookup_entry(state: &RegistryState, resource_id: ResourceId) -> Option<&Arc<Entry>> {
    lookup_slot(state, resource_id).map(|(_, entry)| entry)
}

/// Reports whether any registered dependent still names this resource as a
/// prerequisite.
///
/// # Parameters
///
/// - `state`: Dependency edges to inspect.
/// - `resource_id`: Resource whose dependent references are checked.
///
/// # Returns
///
/// `true` when at least one dependency set contains `resource_id`.
#[must_use]
pub(in crate::resource) fn has_unreleased_prerequisite(state: &RegistryState, resource_id: ResourceId) -> bool {
    state
        .dependencies
        .values()
        .any(|dependents| dependents.contains(&resource_id))
}

/// Collects every resource connected to `start` by prerequisite/dependent
/// edges.
///
/// # Parameters
///
/// - `state`: Dependency graph to traverse.
/// - `start`: Resource from which to search in both edge directions.
///
/// # Returns
///
/// The connected resource identities in sorted, duplicate-free order.
#[must_use]
pub(in crate::resource) fn connected_group(state: &RegistryState, start: ResourceId) -> Vec<ResourceId> {
    let mut visited = BTreeSet::new();
    let mut pending = VecDeque::from([start]);
    while let Some(current) = pending.pop_front() {
        if !visited.insert(current) {
            continue;
        }
        if let Some(dependents) = state.dependencies.get(&current) {
            pending.extend(dependents.iter().copied());
        }
        for (prerequisite, dependents) in &state.dependencies {
            if dependents.contains(&current) {
                pending.push_back(*prerequisite);
            }
        }
    }
    visited.into_iter().collect()
}

/// Selects available, non-exhausted registry slots up to the requested count.
///
/// # Parameters
///
/// - `state`: Registry slots to inspect.
/// - `count`: Maximum number of indices to return.
///
/// # Returns
///
/// Free slot indices in ascending order; fewer than `count` are returned when
/// the registry has insufficient capacity.
#[must_use]
pub(in crate::resource) fn free_slot_indices(state: &RegistryState, count: usize) -> Vec<usize> {
    state
        .slots
        .iter()
        .enumerate()
        .filter(|(_, slot)| slot.entry.is_none() && !slot.exhausted)
        .take(count)
        .map(|(index, _)| index)
        .collect()
}

/// Checks whether following prerequisite-to-dependent edges can reach `target`.
///
/// # Parameters
///
/// - `state`: Directed dependency graph to traverse.
/// - `from`: Resource at which traversal starts.
/// - `target`: Resource whose reachability is tested.
///
/// # Returns
///
/// `true` when `target` is `from` or is reachable through one or more directed
/// edges; otherwise returns `false`.
#[must_use]
pub(in crate::resource) fn reachable(state: &RegistryState, from: ResourceId, target: ResourceId) -> bool {
    let mut pending = vec![from];
    let mut visited = BTreeSet::new();
    while let Some(current) = pending.pop() {
        if current == target {
            return true;
        }
        if !visited.insert(current) {
            continue;
        }
        if let Some(next) = state.dependencies.get(&current) {
            pending.extend(next.iter().copied());
        }
    }
    false
}

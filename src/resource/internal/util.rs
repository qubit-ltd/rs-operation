// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::sync::Arc;

use super::Entry;
use super::RegistryState;
use crate::report::ResourceId;

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

pub(in crate::resource) fn lookup_entry(state: &RegistryState, resource_id: ResourceId) -> Option<&Arc<Entry>> {
    lookup_slot(state, resource_id).map(|(_, entry)| entry)
}

pub(in crate::resource) fn has_unreleased_prerequisite(state: &RegistryState, resource_id: ResourceId) -> bool {
    state
        .dependencies
        .values()
        .any(|dependents| dependents.contains(&resource_id))
}

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

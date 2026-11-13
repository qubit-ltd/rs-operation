// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod entry;
mod entry_phase;
mod entry_state;
mod recovery_inner;
mod registry_inner;
mod registry_state;
mod slot;
mod util;

pub(in crate::resource) use entry::Entry;
pub(in crate::resource) use entry_phase::EntryPhase;
pub(in crate::resource) use entry_state::EntryState;
pub(in crate::resource) use recovery_inner::RecoveryInner;
pub(in crate::resource) use registry_inner::RegistryInner;
pub(in crate::resource) use registry_state::RegistryState;
pub(in crate::resource) use slot::Slot;
pub(in crate::resource) use util::connected_group;
pub(in crate::resource) use util::free_slot_indices;
pub(in crate::resource) use util::has_unreleased_prerequisite;
pub(in crate::resource) use util::lookup_entry;
pub(in crate::resource) use util::lookup_slot;
pub(in crate::resource) use util::reachable;

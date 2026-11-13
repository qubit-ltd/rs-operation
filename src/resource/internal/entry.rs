// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;
use std::sync::Weak;

use super::EntryState;
use super::RegistryInner;
use crate::protocol::OwnedCleanup;

/// Tracks one resource's registry association and cleanup owner.
///
/// The entry is shared by the resource registry and its handles; each field
/// is independently synchronized because registry callbacks and handle use
/// may overlap.
pub(in crate::resource) struct Entry {
    /// Weak link to the registry currently responsible for this resource.
    pub(in crate::resource) registry: Mutex<Weak<RegistryInner>>,
    /// Mutable lifecycle and transfer state for this resource.
    pub(in crate::resource) state: Mutex<EntryState>,
    /// Cleanup callback retained until the resource's cleanup responsibility
    /// is transferred or completed.
    pub(in crate::resource) owner: Mutex<Option<Box<dyn OwnedCleanup>>>,
}

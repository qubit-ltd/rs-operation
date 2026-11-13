// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::EntryPhase;
use crate::report::ResourceId;

/// Tracks one registered resource's lifecycle and cleanup-transfer bookkeeping.
///
/// The state is protected by the owning entry's mutex. Its fields coordinate
/// use leases, cleanup phases, and receipt-validated transfers between
/// registries.
pub(in crate::resource) struct EntryState {
    /// Identifies this resource generation within its operation.
    pub(in crate::resource) id: ResourceId,
    /// Records whether the resource accepts use, is being cleaned, or has
    /// completed or transferred cleanup.
    pub(in crate::resource) phase: EntryPhase,
    /// Counts outstanding use leases that must end before cleanup can start.
    pub(in crate::resource) active_uses: usize,
    /// Identifies the source resource when this registration accepted an
    /// owned cleanup transfer.
    pub(in crate::resource) transfer_source: Option<ResourceId>,
    /// Identifies the receiver that confirmed this resource's cleanup transfer.
    pub(in crate::resource) transfer_receiver: Option<ResourceId>,
}

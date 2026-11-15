// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashSet;
use std::collections::VecDeque;

use crate::v02::report::ResourceId;
use crate::v02::resource::cleanup_port::CleanupJob;

/// Tracks bounded cleanup jobs transferred from resource scopes.
///
/// The state is protected by the receiver's mutex. Reservations count queued
/// and in-flight jobs against capacity; resource IDs prevent duplicate
/// transfers until their jobs complete or are drained during shutdown.
pub(in crate::v02::resource::cleanup) struct TransferQueueState {
    /// Whether new cleanup jobs may be accepted.
    pub(in crate::v02::resource::cleanup) accepting: bool,
    /// Number of queue-capacity slots currently reserved by transferred jobs.
    pub(in crate::v02::resource::cleanup) reserved: usize,
    /// FIFO jobs awaiting dispatch, paired with the resource each job cleans
    /// up.
    pub(in crate::v02::resource::cleanup) pending: VecDeque<(ResourceId, Box<dyn CleanupJob>)>,
    /// Resource IDs already accepted and not yet released by the dispatcher.
    pub(in crate::v02::resource::cleanup) ids: HashSet<ResourceId>,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::sync::Arc;

use crate::v02::report::RecoveryId;
use crate::v02::report::RecoveryPhase;
use crate::v02::resource::recovery::RecoveryCore;

/// Admission state and retained groups owned by a recovery store.
///
/// Resolved groups may remain in the map for observation, but they no longer
/// count against active recovery group or resource capacity.
pub(in crate::v02::resource::recovery) struct RecoveryStoreState {
    /// Whether the store accepts additional transferred recovery groups.
    pub(in crate::v02::resource::recovery) accepting: bool,
    /// Strong references that keep transferred groups available for retry.
    pub(in crate::v02::resource::recovery) groups: HashMap<RecoveryId, Arc<RecoveryCore>>,
}

/// Counts unresolved recovery groups and their retained resource identities.
///
/// Each group is counted from its current recovery snapshot; resolved groups
/// contribute neither a group nor resource capacity.
///
/// # Parameters
///
/// * `state` — store state whose transferred groups are measured.
///
/// # Returns
///
/// A tuple of `(active_group_count, active_resource_count)`.
pub(in crate::v02::resource::recovery) fn store_usage(state: &RecoveryStoreState) -> (usize, usize) {
    state
        .groups
        .values()
        .map(|group| {
            let phase = group.snapshot().phase;
            let active = phase != RecoveryPhase::Resolved;
            (usize::from(active), if active { group.resource_count() } else { 0 })
        })
        .fold((0, 0), |(groups, resources), (group, resource)| {
            (groups + group, resources + resource)
        })
}

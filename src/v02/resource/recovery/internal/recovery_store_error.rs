// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Reason a recovery store rejected a transferred group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryStoreError {
    /// The store no longer accepts groups.
    Closed,
    /// The store has no free group slot.
    GroupCapacity,
    /// The store lacks resource capacity.
    ResourceCapacity,
    /// The group was already transferred.
    AlreadyTransferred,
    /// The group is empty, duplicated, or spans managers.
    InvalidGroup,
}

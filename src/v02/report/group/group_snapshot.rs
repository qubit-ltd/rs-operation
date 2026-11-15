// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::GroupId;

/// Point-in-time state for a manager operation group.
///
/// # Examples
///
/// Check whether a group snapshot indicates an active service scope:
///
/// ```
/// use qubit_operation::GroupSnapshot;
///
/// fn has_active_service(snapshot: &GroupSnapshot) -> bool {
///     snapshot.service_resource_scopes > 0
/// }
/// ```
#[derive(Clone, Debug)]
pub struct GroupSnapshot {
    /// Identity of the group.
    pub id: GroupId,
    /// Whether the group accepts new child operations.
    pub accepting: bool,
    /// Number of operations currently executing in the group.
    pub active_operations: usize,
    /// Number of resource scopes retained by group services.
    pub service_resource_scopes: usize,
}

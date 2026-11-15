// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Current owner-scope state relevant to resource cleanup.
///
/// # Examples
///
/// ```
/// use qubit_operation::ResourceScopeSnapshot;
///
/// fn may_cleanup(snapshot: &ResourceScopeSnapshot) -> bool {
///     snapshot.owner_scope_ended && snapshot.user_references == 0
/// }
///
/// let snapshot = ResourceScopeSnapshot {
///     owner_scope_ended: true,
///     user_references: 0,
///     cleanup_started: 0,
///     pending_resources: 0,
/// };
/// assert!(may_cleanup(&snapshot));
/// ```
#[derive(Clone, Debug)]
pub struct ResourceScopeSnapshot {
    /// Whether the scope that owns the resources has ended.
    pub owner_scope_ended: bool,
    /// Number of outstanding user references delaying cleanup.
    pub user_references: usize,
    /// Number of resources whose cleanup has begun.
    pub cleanup_started: usize,
    /// Number of resources still awaiting cleanup or release evidence.
    pub pending_resources: usize,
}

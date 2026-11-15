// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Point-in-time counts of manager-owned operations and resources.
///
/// # Examples
///
/// ```
/// use qubit_operation::ManagerSnapshot;
///
/// fn has_work(snapshot: &ManagerSnapshot) -> bool {
///     snapshot.active_operations > 0 || snapshot.queued_operations > 0
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ManagerSnapshot {
    /// Operations currently executing.
    pub active_operations: usize,
    /// Operations waiting for executor capacity.
    pub queued_operations: usize,
    /// Callers waiting for operation admission.
    pub admission_waiters: usize,
    /// Completed results not yet consumed.
    pub unconsumed_outcomes: usize,
    /// Resources currently undergoing cleanup.
    pub cleaning_resources: usize,
    /// Recovery groups retained for later action.
    pub recovery_groups: usize,
    /// Resource users whose release is not yet confirmed.
    pub unconfirmed_users: usize,
    /// Diagnostic events omitted after the configured cap was reached.
    pub diagnostic_overflow: usize,
    /// Resources whose owning scope remains open.
    pub scope_open_resources: usize,
    /// Resources waiting for user references to be released.
    pub waiting_for_users_resources: usize,
}

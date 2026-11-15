// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Counts of outstanding work and resources during manager shutdown.
///
/// # Examples
///
/// ```
/// use qubit_operation::ShutdownSnapshot;
///
/// let snapshot = ShutdownSnapshot {
///     active_operations: 0,
///     queued_operations: 0,
///     unconfirmed_users: 0,
///     cleaning_resources: 0,
///     recovery_groups: 0,
///     service_resources: 0,
/// };
/// assert_eq!(snapshot.active_operations, 0);
/// ```
#[must_use]
#[derive(Clone, Debug)]
pub struct ShutdownSnapshot {
    /// Operations still executing.
    pub active_operations: usize,
    /// Operations still queued for execution.
    pub queued_operations: usize,
    /// Users whose resource release is not confirmed.
    pub unconfirmed_users: usize,
    /// Resources whose cleanup is still running.
    pub cleaning_resources: usize,
    /// Recovery groups still owned by the manager.
    pub recovery_groups: usize,
    /// Service resources still held by registered services.
    pub service_resources: usize,
}

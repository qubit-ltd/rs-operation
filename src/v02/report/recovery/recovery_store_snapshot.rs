// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Point-in-time capacity usage and admission state for a recovery store.
///
/// Values are returned by
/// [`RecoveryStore::snapshot`](crate::RecoveryStore::snapshot). Resolved groups
/// are pruned before counts are collected.
///
/// # Examples
///
/// ```
/// use qubit_operation::{RecoveryStore, RecoveryStoreOptions, RecoveryStoreSnapshot};
///
/// fn retained_resources(snapshot: &RecoveryStoreSnapshot) -> usize {
///     snapshot.resources
/// }
///
/// let store = RecoveryStore::new(RecoveryStoreOptions::default())?;
/// let snapshot = store.snapshot();
/// assert!(snapshot.accepting);
/// assert_eq!(retained_resources(&snapshot), 0);
/// drop(
///     store
///         .request_shutdown()
///         .wait()
///         .expect("recovery store shutdown succeeds"),
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Debug)]
pub struct RecoveryStoreSnapshot {
    /// Whether the store accepts additional recovery records.
    pub accepting: bool,
    /// Number of unresolved recovery groups retained after pruning resolved
    /// groups.
    pub groups: usize,
    /// Number of resources across the retained recovery groups.
    pub resources: usize,
    /// Number of retained groups currently in the running phase.
    pub running_attempts: usize,
}

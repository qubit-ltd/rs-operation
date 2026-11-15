// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Capacity limits for manager-wide operation and diagnostic state.
///
/// # Examples
///
/// ```
/// use qubit_operation::ManagerLimits;
///
/// let limits = ManagerLimits::default();
/// assert!(limits.active_operations <= limits.queued_operations);
/// assert!(limits.cleanup_concurrency > 0);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagerLimits {
    /// Maximum operations executing at once.
    pub active_operations: usize,
    /// Maximum operations waiting in the manager queue.
    pub queued_operations: usize,
    /// Maximum callers waiting for admission capacity.
    pub admission_waiters: usize,
    /// Maximum completed outcomes retained before they are consumed.
    pub unconsumed_outcomes: usize,
    /// Maximum cleanup jobs running concurrently.
    pub cleanup_concurrency: usize,
    /// Maximum diagnostic events retained by the manager.
    pub diagnostic_events: usize,
}

impl Default for ManagerLimits {
    /// Constructs the default configuration for [`ManagerLimits`].
    fn default() -> Self {
        Self {
            active_operations: 4,
            queued_operations: 64,
            admission_waiters: 64,
            unconsumed_outcomes: 64,
            cleanup_concurrency: 4,
            diagnostic_events: 256,
        }
    }
}

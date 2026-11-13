// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::ScopeLimits;

/// Supervisor-wide finite queue, worker, and report capacities from the 0.1
/// design baseline.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::SupervisorLimits;
///
/// let limits = SupervisorLimits {
///     active_roots: 8,
///     ..SupervisorLimits::default()
/// };
/// assert_eq!(limits.active_roots, 8);
/// assert_eq!(limits.cleanup_concurrency, 4);
/// assert_eq!(limits.scope.resources, 128);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SupervisorLimits {
    /// Maximum concurrently active root scopes.
    pub active_roots: usize,
    /// Maximum root bodies accepted but waiting to run.
    pub queued_roots: usize,
    /// Maximum callers waiting for admission; excess callers receive `Busy`.
    pub admission_waiters: usize,
    /// Maximum concurrent cleanup tasks, separate from business admission.
    pub cleanup_concurrency: usize,
    /// Maximum retained diagnostic events; overflow must remain observable.
    pub unobserved_events: usize,
    /// Maximum reserved but unconsumed typed results.
    pub unobserved_results: usize,
    /// Default per-root child and resource capacities.
    pub scope: ScopeLimits,
}

impl Default for SupervisorLimits {
    /// Creates the baseline supervisor capacities used by a new report.
    ///
    /// The limits bound active and queued roots, admission waiters, cleanup
    /// work, retained diagnostics, and unconsumed results independently.
    ///
    /// # Returns
    ///
    /// A supervisor configuration with the baseline capacities.
    #[inline]
    fn default() -> Self {
        Self {
            active_roots: 4,
            queued_roots: 64,
            admission_waiters: 64,
            cleanup_concurrency: 4,
            unobserved_events: 256,
            unobserved_results: 64,
            scope: ScopeLimits::default(),
        }
    }
}

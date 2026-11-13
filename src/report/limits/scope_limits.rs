// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Finite capacity reserved for one root operation and its descendants.
///
/// Per-root finite registration capacity. Exhaustion rejects registration
/// before work starts.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::ScopeLimits;
///
/// let limits = ScopeLimits {
///     resources: 32,
///     ..ScopeLimits::default()
/// };
/// assert_eq!(limits.resources, 32);
/// assert_eq!(limits.child_operations, 64);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeLimits {
    /// Maximum child-operation registrations, including attached but unreleased
    /// children.
    pub child_operations: usize,
    /// Maximum resource registrations, including resources retained for
    /// recovery.
    pub resources: usize,
    /// Maximum caller-declared side-effect records retained in the root
    /// settlement.
    pub effect_records: usize,
}

impl Default for ScopeLimits {
    /// Creates the standard registration capacities for one root operation.
    ///
    /// These defaults reserve capacity for 64 child operations, 128 resources,
    /// and 64 caller-declared side-effect records. They bound registrations
    /// without changing the limits explicitly chosen by callers.
    #[inline]
    fn default() -> Self {
        Self {
            child_operations: 64,
            resources: 128,
            effect_records: 64,
        }
    }
}

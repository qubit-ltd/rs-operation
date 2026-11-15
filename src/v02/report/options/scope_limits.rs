// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Capacity limits inherited by an operation's resource scope.
///
/// # Examples
///
/// ```
/// use qubit_operation::ScopeLimits;
///
/// let limits = ScopeLimits::default();
/// assert!(limits.resources > 0);
/// assert!(limits.effect_records > 0);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScopeLimits {
    /// Maximum child operations admitted into the scope.
    pub child_operations: usize,
    /// Maximum resources registered with the scope.
    pub resources: usize,
    /// Maximum child resource scopes attached to this operation scope.
    pub resource_scopes: usize,
    /// Maximum effect records retained for the scope.
    pub effect_records: usize,
}

impl Default for ScopeLimits {
    /// Constructs the default configuration for [`ScopeLimits`].
    fn default() -> Self {
        Self {
            child_operations: 64,
            resources: 128,
            resource_scopes: 64,
            effect_records: 64,
        }
    }
}

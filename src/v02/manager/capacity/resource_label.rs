// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::ManagerCapacity;
use super::ScopeCapacityKind;

/// A finite manager or operation-tree capacity backed by `qubit-budget`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResourceLabel {
    /// Descriptive label used by standalone pools and tests.
    Named(
        /// Static diagnostic name attached to a standalone pool.
        &'static str,
    ),
    /// Identifies a manager-wide pool kind.
    Manager(
        /// Manager capacity class represented by this label.
        ManagerCapacity,
    ),
    /// Identifies a scope-local pool and its domain.
    Scope {
        /// Stable raw identity of the operation or resource scope.
        scope_id: u128,
        /// Capacity class enforced for this scope identity.
        capacity: ScopeCapacityKind,
    },
}

impl From<&'static str> for ResourceLabel {
    /// Uses the string as a diagnostic label for the pool.
    ///
    /// # Parameters
    ///
    /// - `value`: static diagnostic label for the capacity pool.
    ///
    /// # Returns
    ///
    /// A named `ResourceLabel` containing `value`.
    #[inline]
    fn from(value: &'static str) -> Self {
        Self::Named(value)
    }
}

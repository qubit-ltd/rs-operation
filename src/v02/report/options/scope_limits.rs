// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Capacity limits requested for an operation's scope and inherited by its
/// descendants.
///
/// The default limits are 64 child operations, 128 resources, 64 attached
/// resource scopes, and 64 effect records. For an operation, a complete value
/// equal to [`ScopeLimits::default()`] is the sentinel that selects the
/// manager's `scope_limits` value as the operation's limits. A value that
/// differs in any field is used as a whole; unchanged fields are not merged
/// from the manager configuration. This means an explicit request for the
/// complete default values cannot be distinguished from inheritance.
///
/// A root operation can therefore override the manager defaults by supplying
/// any non-default value. A child operation's requested capacities are
/// clamped along the parent chain, so no descendant can exceed an ancestor's
/// effective limit. These limits bound simultaneous retained capacity; a
/// released reservation can be used again.
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
    /// Maximum simultaneously admitted child operations; defaults to `64`.
    pub child_operations: usize,
    /// Maximum simultaneously registered resources; defaults to `128`.
    pub resources: usize,
    /// Maximum attached child resource scopes; defaults to `64`.
    pub resource_scopes: usize,
    /// Maximum retained effect records; defaults to `64`.
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

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Configuration accepted when a root or child scope is created.

use super::ScopePolicy;
use crate::context::ContextOptions;
use crate::report::ScopeLimits;

/// Options fixed when a root or child scope is created.
///
/// # Examples
///
/// ```
/// use qubit_operation::scope::{ScopeOptions, ScopePolicy};
///
/// let mut options = ScopeOptions::default();
/// options.limits.resources = 32;
/// options.policy = ScopePolicy::Collect;
///
/// assert_eq!(options.limits.resources, 32);
/// assert_eq!(options.policy, ScopePolicy::Collect);
/// assert!(options.context.clock.is_none());
/// ```
#[derive(Clone, Default)]
pub struct ScopeOptions {
    /// Upper bounds for children, resources, and retained effect records.
    pub limits: ScopeLimits,
    /// Policy applied when a required child reports failure.
    pub policy: ScopePolicy,
    /// Deadline, budget, and cancellation context inherited by the scope.
    pub context: ContextOptions,
}

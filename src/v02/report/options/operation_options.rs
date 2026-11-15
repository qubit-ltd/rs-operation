// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::ChildFailurePolicy;
use crate::v02::report::ChildRequirement;
use crate::v02::report::ContextOptions;
use crate::v02::report::ScopeLimits;

/// Per-child policy and context inherited when an operation starts a child.
///
/// # Examples
///
/// ```
/// use qubit_operation::{ChildRequirement, OperationOptions};
///
/// let options = OperationOptions::default();
/// assert_eq!(options.requirement, ChildRequirement::Required);
/// ```
#[derive(Clone)]
pub struct OperationOptions {
    /// Whether failure of this child affects its parent outcome.
    pub requirement: ChildRequirement,
    /// Limits applied to the child operation's scope.
    pub limits: ScopeLimits,
    /// Sibling behavior after a required child fails.
    pub child_failure_policy: ChildFailurePolicy,
    /// Clock, deadline, or budget overrides for this child.
    pub context: ContextOptions,
}

impl Default for OperationOptions {
    /// Constructs the default configuration for [`OperationOptions`].
    fn default() -> Self {
        Self {
            requirement: ChildRequirement::Required,
            limits: ScopeLimits::default(),
            child_failure_policy: ChildFailurePolicy::StopSiblings,
            context: ContextOptions::default(),
        }
    }
}

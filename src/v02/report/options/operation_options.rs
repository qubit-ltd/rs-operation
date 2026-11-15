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
/// `limits` is compared as a complete [`ScopeLimits`] value. When it equals
/// [`ScopeLimits::default()`], submission substitutes the manager's
/// `scope_limits` value for the whole structure, including for root
/// operations. Any other value is used as the operation's requested limits;
/// fields are not merged with manager defaults. For child operations, each
/// requested capacity is then clamped by the effective limits of every
/// ancestor. Because the complete default value is the inheritance sentinel,
/// callers cannot express an explicit request for exactly that value.
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
    /// Requested capacity limits for this operation's scope. The complete
    /// [`ScopeLimits::default()`] value means inherit the manager's
    /// `scope_limits`; any other value replaces that default as a whole.
    /// Effective child limits are clamped by every ancestor.
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

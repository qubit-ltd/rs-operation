// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::protocol::ClockSource;
use crate::v02::report::ManagerLimits;
use crate::v02::report::PortableExecutorOptions;
use crate::v02::report::ScopeLimits;
use crate::v02::report::ThreadPoolExecutorOptions;

/// Manager-wide configuration for limits, executors, and clock selection.
///
/// `scope_limits` supplies the default capacity limits for each root operation.
/// A root submitted with `OperationOptions::default()` inherits this complete
/// structure. A non-default `OperationOptions::limits` replaces the manager
/// default as a whole; its fields are not merged individually. Descendant
/// operations can request smaller or larger limits, but their effective
/// capacities are clamped by every ancestor's limits. Since default-valued
/// operation limits mean "inherit", an explicit request for the complete
/// `ScopeLimits::default()` cannot be distinguished from inheritance.
///
/// # Examples
///
/// ```
/// use qubit_operation::ManagerOptions;
///
/// let options = ManagerOptions::default();
/// assert!(options.limits.active_operations > 0);
/// assert!(options.scope_limits.resources > 0);
/// ```
#[derive(Clone)]
pub struct ManagerOptions {
    /// Limits applied to manager-wide operation state.
    pub limits: ManagerLimits,
    /// Default limits inherited by root operations whose operation limits are
    /// exactly [`ScopeLimits::default()`]. The whole structure is substituted;
    /// individual fields are not merged with an operation's limits. Descendant
    /// limits remain bounded by all ancestors.
    pub scope_limits: ScopeLimits,
    /// Settings for asynchronous task execution.
    pub async_executor: PortableExecutorOptions,
    /// Settings for synchronous operation execution.
    pub sync_executor: ThreadPoolExecutorOptions,
    /// Settings for resource cleanup execution.
    pub cleanup_executor: ThreadPoolExecutorOptions,
    /// Optional manager clock; operation context may override it.
    pub clock: Option<Arc<dyn ClockSource>>,
    /// Maximum distinct service groups that can bind one operation.
    pub max_groups_per_operation: usize,
}

impl Default for ManagerOptions {
    /// Constructs the default configuration for [`ManagerOptions`].
    fn default() -> Self {
        Self {
            limits: ManagerLimits::default(),
            scope_limits: ScopeLimits::default(),
            async_executor: PortableExecutorOptions::default(),
            sync_executor: ThreadPoolExecutorOptions::default(),
            cleanup_executor: ThreadPoolExecutorOptions::default(),
            clock: None,
            max_groups_per_operation: 16,
        }
    }
}

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
    /// Default limits for scopes created by operations.
    pub scope_limits: ScopeLimits,
    /// Settings for asynchronous task execution.
    pub async_executor: PortableExecutorOptions,
    /// Settings for synchronous operation execution.
    pub sync_executor: ThreadPoolExecutorOptions,
    /// Settings for resource cleanup execution.
    pub cleanup_executor: ThreadPoolExecutorOptions,
    /// Optional manager clock; operation context may override it.
    pub clock: Option<Arc<dyn ClockSource>>,
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
        }
    }
}

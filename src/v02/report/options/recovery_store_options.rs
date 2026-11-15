// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::ThreadPoolExecutorOptions;

/// Capacity and cleanup settings for a recovery store.
///
/// # Examples
///
/// ```
/// use qubit_operation::RecoveryStoreOptions;
///
/// let options = RecoveryStoreOptions::default();
/// assert!(options.group_capacity > 0);
/// assert!(options.resource_capacity >= options.group_capacity);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryStoreOptions {
    /// Maximum recovery groups retained by the store.
    pub group_capacity: usize,
    /// Maximum resources retained across recovery groups.
    pub resource_capacity: usize,
    /// Maximum cleanup attempts the store runs concurrently.
    pub cleanup_concurrency: usize,
    /// Worker and queue settings for the recovery cleanup executor.
    pub cleanup_executor: ThreadPoolExecutorOptions,
}

impl Default for RecoveryStoreOptions {
    /// Constructs the default configuration for [`RecoveryStoreOptions`].
    fn default() -> Self {
        Self {
            group_capacity: 64,
            resource_capacity: 128,
            cleanup_concurrency: 4,
            cleanup_executor: ThreadPoolExecutorOptions::default(),
        }
    }
}

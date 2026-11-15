// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Worker and queue settings for a blocking thread-pool executor.
///
/// # Examples
///
/// ```
/// use qubit_operation::ThreadPoolExecutorOptions;
///
/// let options = ThreadPoolExecutorOptions::default();
/// assert!(options.worker_count > 0);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ThreadPoolExecutorOptions {
    /// Number of blocking worker threads.
    pub worker_count: usize,
    /// Number of tasks that may wait for a worker.
    pub queue_capacity: usize,
}

impl Default for ThreadPoolExecutorOptions {
    /// Constructs the default configuration for [`ThreadPoolExecutorOptions`].
    fn default() -> Self {
        Self {
            worker_count: 4,
            queue_capacity: 64,
        }
    }
}

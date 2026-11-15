// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Worker and queue settings for the portable asynchronous executor.
///
/// # Examples
///
/// ```
/// use qubit_operation::PortableExecutorOptions;
///
/// let options = PortableExecutorOptions::default();
/// assert!(options.worker_count > 0);
/// assert!(options.polls_per_turn > 0);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortableExecutorOptions {
    /// Number of executor workers to run.
    pub worker_count: usize,
    /// Maximum queued tasks, including the configured in-flight allowance.
    pub task_capacity: usize,
    /// Maximum polls performed by a worker before yielding its turn.
    pub polls_per_turn: usize,
}

impl Default for PortableExecutorOptions {
    /// Constructs the default configuration for [`PortableExecutorOptions`].
    fn default() -> Self {
        Self {
            worker_count: 4,
            task_capacity: 68,
            polls_per_turn: 16,
        }
    }
}

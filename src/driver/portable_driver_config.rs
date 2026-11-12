// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Worker and capacity limits for a portable driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortableDriverConfig {
    /// Maximum number of worker threads.
    pub worker_count: usize,
    /// Maximum number of accepted, unfinished tasks.
    pub task_capacity: usize,
    /// Maximum tasks polled by a worker before it yields its turn.
    pub polls_per_turn: usize,
}

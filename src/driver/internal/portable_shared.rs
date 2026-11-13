// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Condvar;
use std::sync::Mutex;

use super::PortableState;

/// Shared portable scheduler state referenced weakly by task wakers.
pub struct PortableShared {
    /// Task registry and ready queue guarded by the scheduler mutex.
    pub state: Mutex<PortableState>,
    /// Wakes workers when tasks become ready or shutdown begins.
    pub ready: Condvar,
    /// Maximum number of unfinished tasks accepted by the driver.
    pub task_capacity: usize,
    /// Maximum number of tasks a worker polls before yielding its turn.
    pub polls_per_turn: usize,
    #[cfg(test)]
    pub active_workers: std::sync::atomic::AtomicUsize,
}

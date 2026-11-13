// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Condvar;
use std::sync::Mutex;

use super::ThreadState;

/// Synchronization state shared between an executor and its worker threads.
pub struct ThreadShared {
    /// Holds the pending-job queue and shutdown flag shared by all workers.
    pub state: Mutex<ThreadState>,
    /// Wakes workers after jobs are queued or the executor is closed.
    pub ready: Condvar,
    /// Limits how many jobs may wait in the queue at once.
    pub queue_capacity: usize,
    /// Identifies this executor on its worker threads.
    pub executor_id: u64,
}

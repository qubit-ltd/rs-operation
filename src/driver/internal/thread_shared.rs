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
    pub state: Mutex<ThreadState>,
    pub ready: Condvar,
    pub queue_capacity: usize,
    pub executor_id: u64,
}

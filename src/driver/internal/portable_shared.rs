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
    pub state: Mutex<PortableState>,
    pub ready: Condvar,
    pub task_capacity: usize,
    pub polls_per_turn: usize,
}

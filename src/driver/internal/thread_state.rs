// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::VecDeque;

use super::ThreadJob;

/// Pending jobs and close state protected by the executor mutex.
pub struct ThreadState {
    pub queue: VecDeque<ThreadJob>,
    pub closed: bool,
}

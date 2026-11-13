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
    /// Jobs waiting to run, in the order they were submitted.
    ///
    /// Access this queue only while holding the owning executor's state mutex.
    pub queue: VecDeque<ThreadJob>,
    /// Whether the executor has stopped accepting new jobs.
    ///
    /// This flag is coordinated with `queue` under the same state mutex.
    pub closed: bool,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Admission and ready-task queue state for the portable executor.

use std::collections::VecDeque;
use std::sync::Arc;

use super::PortableTask;

/// Mutable scheduling state protected by the portable executor mutex.
pub(in crate::v02::executor) struct PortableState {
    /// Whether the executor accepts new task reservations.
    pub(in crate::v02::executor) accepting: bool,
    /// Number of tasks retained by the executor, including pending tasks.
    pub(in crate::v02::executor) tasks: usize,
    /// Tasks whose wakers have marked them ready for another poll.
    pub(in crate::v02::executor) ready: VecDeque<Arc<PortableTask>>,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! RAII guard that decrements the pool's active-poll counter when dropped.

use std::sync::Mutex;

use super::lock;

/// Records one active poll that must be released when polling completes.
pub(in crate::v02::executor) struct PollSlot<'a>(
    /// Counter shared with the pool state.
    pub(in crate::v02::executor) &'a Mutex<usize>,
);

impl Drop for PollSlot<'_> {
    /// Decrements the active-poll count exactly once.
    fn drop(&mut self) {
        *lock(self.0) -= 1;
    }
}

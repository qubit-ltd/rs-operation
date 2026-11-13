// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

use super::CleanupTaskInner;

/// Synchronization wrapper shared by observers of one cleanup task.
pub(in crate::resource::cleanup_scheduler) struct CleanupTaskState {
    /// Mutable task data protected while the future is moved out for polling.
    pub(in crate::resource::cleanup_scheduler) inner: Mutex<CleanupTaskInner>,
}

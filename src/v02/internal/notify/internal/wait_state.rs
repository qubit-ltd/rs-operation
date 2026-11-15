// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Generation and task-waker registrations shared by notification observers.

use std::task::Waker;

/// Mutable state protected by the notification source mutex.
#[derive(Default)]
pub(in crate::v02::internal::notify) struct WaitState {
    /// Current generation used to determine whether an observer is ready.
    pub(in crate::v02::internal::notify) generation: u64,
    /// Async task IDs and their wakers, drained when a generation is published.
    pub(in crate::v02::internal::notify) waiters: Vec<(u64, Waker)>,
}

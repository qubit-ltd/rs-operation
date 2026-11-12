// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Generation and async waiter state guarded by one notification source.

use std::task::Waker;

/// Stores the generation observed by waiters and their registered wakers.
#[derive(Default)]
pub(super) struct WaitState {
    /// Incremented whenever state becomes observable to waiters.
    pub(super) generation: u64,
    /// Async waiters registered for the current generation.
    pub(super) waiters: Vec<(u64, Waker)>,
}

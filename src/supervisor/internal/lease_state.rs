// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutable state for one root admission reservation.

use std::task::Waker;

/// Tracks one lease's queue membership, active capacity, and result reservation.
#[derive(Default)]
pub(super) struct LeaseState {
    /// Whether this lease is waiting in the bounded root-admission queue.
    pub(in crate::supervisor) queued: bool,
    /// Whether this lease currently occupies an active root slot.
    pub(in crate::supervisor) active: bool,
    /// Makes execution release idempotent across cancellation and drop paths.
    pub(in crate::supervisor) execution_released: bool,
    /// Whether result capacity remains reserved for this operation.
    pub(in crate::supervisor) result_reserved: bool,
    /// Wakers waiting for a capacity change associated with this lease.
    pub(in crate::supervisor) active_waiters: Vec<Waker>,
}


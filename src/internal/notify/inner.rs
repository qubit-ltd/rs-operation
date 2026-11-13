// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared synchronization backing one notification source.

use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;

use super::wait_state::WaitState;

/// Holds the generation, waiter list, and blocking notification primitive.
#[derive(Default)]
pub(super) struct Inner {
    /// Serializes generation and waiter registration.
    pub(super) state: Mutex<WaitState>,
    /// Wakes blocking observers after a generation change.
    pub(super) changed: Condvar,
    /// Allocates distinct waiter identities for independent futures.
    pub(super) next_waiter: AtomicU64,
}

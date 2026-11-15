// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Shared notification state used by asynchronous and blocking observers.

use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;

use super::WaitState;

/// Shared state behind a notification source.
///
/// The mutex protects the generation and async waiter list; the condition
/// variable wakes blocking observers, while the atomic assigns async waiter
/// IDs.
#[derive(Default)]
pub(in crate::v02::internal::notify) struct Inner {
    /// Generation and registered task wakers protected as one state snapshot.
    pub(in crate::v02::internal::notify) state: Mutex<WaitState>,
    /// Wakes blocking observers after a generation change.
    pub(in crate::v02::internal::notify) changed: Condvar,
    /// Monotonic source for identifying async waiter registrations.
    pub(in crate::v02::internal::notify) next_waiter: AtomicU64,
}

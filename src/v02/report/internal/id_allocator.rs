// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Supplies process-local identities for opaque report entities.
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// Allocates the next process-local opaque identity using an atomic counter.
///
/// Relaxed ordering makes concurrent allocations unique without imposing
/// synchronization or a happens-before relationship on the operations using
/// those identities.
pub(in crate::v02::report) fn next_id() -> u128 {
    u128::from(NEXT_ID.fetch_add(1, Ordering::Relaxed))
}

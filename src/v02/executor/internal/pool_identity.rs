// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::cell::Cell;
use std::sync::atomic::AtomicU64;

/// Allocates distinct identifiers used to mark executor worker threads.
pub(in crate::v02::executor) static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    /// Pool whose worker is running on this thread, or zero outside a worker.
    pub(in crate::v02::executor) static CURRENT_POOL_ID: Cell<u64> = const { Cell::new(0) };
}

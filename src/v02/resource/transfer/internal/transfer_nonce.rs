// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::atomic::AtomicU64;

/// Allocates process-wide nonce values for resource transfer receipts.
///
/// The counter is incremented atomically by concurrent transfers. Relaxed
/// ordering is sufficient because it provides identifier allocation without
/// publishing or synchronizing any other state.
pub(in crate::v02::resource::transfer) static NEXT_TRANSFER_NONCE: AtomicU64 = AtomicU64::new(1);

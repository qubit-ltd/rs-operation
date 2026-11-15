// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Provides process-wide nonce values for recovery-transfer receipts.
static NEXT_RECOVERY_TRANSFER: AtomicU64 = AtomicU64::new(1);

/// Allocates a nonce for a recovery-transfer receipt.
///
/// Relaxed ordering is sufficient because this atomic allocates identifiers;
/// it does not synchronize access to recovery-transfer state.
///
/// # Returns
///
/// The allocated nonce.
pub(in crate::v02::resource::recovery) fn next_transfer_nonce() -> u64 {
    NEXT_RECOVERY_TRANSFER.fetch_add(1, Ordering::Relaxed)
}

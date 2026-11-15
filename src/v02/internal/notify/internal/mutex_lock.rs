// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

/// Locks internal notification state and continues through mutex poisoning.
///
/// Recovering the guard preserves access to the state after a prior panic; it
/// does not assert that the protected value satisfies higher-level invariants.
///
/// # Type Parameters
///
/// - `T`: state protected by the mutex.
///
/// # Parameters
///
/// - `mutex`: mutex protecting notification state.
///
/// # Returns
///
/// A guard that keeps the mutex locked for the borrow of `mutex`.
pub(in crate::v02::internal::notify) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

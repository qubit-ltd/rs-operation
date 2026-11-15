// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

/// Acquires the state mutex, continuing with its value if it is poisoned.
///
/// # Type Parameters
///
/// - `T`: Value protected by the mutex.
///
/// # Parameters
///
/// * `mutex` is the result-state lock to acquire.
///
/// # Returns
///
/// The guard for the protected state.
pub(in crate::v02::internal::state) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

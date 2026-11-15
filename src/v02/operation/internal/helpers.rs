// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;
use std::sync::MutexGuard;

/// Acquires a mutex guard, recovering the protected value after poisoning.
///
/// # Type Parameters
///
/// - `T`: Value protected by the mutex.
///
/// # Parameters
///
/// - `mutex`: Mutex to acquire.
///
/// # Returns
///
/// The mutex guard, including the inner value if the mutex was poisoned.
pub(in crate::v02::operation) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

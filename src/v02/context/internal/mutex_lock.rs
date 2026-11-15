// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

/// Acquires a mutex and recovers its protected value if it was poisoned.
///
/// # Type Parameters
///
/// * `T` is the value protected by the mutex.
///
/// # Parameters
///
/// * `mutex` is the lock whose protected value is accessed.
pub(in crate::v02::context) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

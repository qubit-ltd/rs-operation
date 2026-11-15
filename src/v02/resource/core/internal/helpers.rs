// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

/// Locks internal resource state while recovering the protected value if
/// poisoned.
///
/// Poisoning records that another thread panicked while holding the mutex. The
/// resource core treats its state as recoverable and continues with the inner
/// value; this helper does not clear the mutex's poison flag.
///
/// # Type Parameters
///
/// - `T`: Type of the state protected by the mutex.
///
/// # Parameters
///
/// - `mutex`: State mutex to lock for exclusive access.
///
/// # Returns
///
/// A guard that keeps the mutex locked until the returned guard is dropped.
pub(in crate::v02::resource::core) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

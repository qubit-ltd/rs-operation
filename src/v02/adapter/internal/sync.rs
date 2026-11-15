// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;
use std::task::Waker;

/// Wakes registered observers after state locks have been released.
///
/// # Parameters
///
/// * `waiters` contains the observer wakers to notify.
pub(in crate::v02::adapter) fn wake_all(waiters: Vec<Waker>) {
    for waker in waiters {
        waker.wake();
    }
}

/// Acquires a mutex while recovering its protected value after poisoning.
///
/// # Type Parameters
///
/// * `T` is the value stored in the mutex.
///
/// # Parameters
///
/// * `mutex` is the shared state lock to acquire.
///
/// # Returns
///
/// A guard that holds the mutex, recovering its inner value if it was poisoned.
pub(in crate::v02::adapter) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

/// Acquires the mutex, recovering its state after a prior holder panicked.
///
/// # Type Parameters
///
/// - `T`: Value protected by the mutex.
///
/// # Parameters
///
/// * `mutex` is the executor state lock to acquire.
///
/// # Returns
///
/// The mutex guard, including the contained value from a poisoned lock.
pub(in crate::v02::executor) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

/// Acquires a mutex guard and keeps access available after poisoning.
///
/// If a previous holder panicked, this helper takes the guard from the poison
/// error so callers can inspect or repair the protected state.
///
/// # Type Parameters
///
/// * `T` — value protected by the mutex.
///
/// # Parameters
///
/// * `mutex` — synchronization primitive whose guard is requested.
///
/// # Returns
///
/// The guard providing exclusive access to the protected value.
pub(in crate::v02::resource::recovery) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

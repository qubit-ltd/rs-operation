// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;

/// Acquires a mutex guard even when the protected value is poisoned.
///
/// A panic in a previous holder sets the poison flag. This helper recovers the
/// guard so transfer code can inspect or repair the state before proceeding.
///
/// # Type Parameters
///
/// * `T` — value protected by the mutex.
///
/// # Parameters
///
/// * `mutex` — mutex whose guard is needed.
///
/// # Returns
///
/// The guard providing exclusive access to the protected value.
#[inline]
pub(in crate::v02::resource::transfer) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

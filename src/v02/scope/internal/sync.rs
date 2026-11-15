// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

/// Recovers a poisoned scope mutex because its state remains inspectable.
///
/// # Type Parameters
///
/// * `T` — the value protected by the mutex.
///
/// # Parameters
///
/// * `mutex` — mutex guarding the scope state.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Latched notifications shared by blocking and asynchronous observers.

use std::sync::Mutex;
use std::sync::MutexGuard;

// Shared synchronization state for notification operations.
mod inner;
// Future that registers an asynchronous waiter.
mod notified;
// Notification source and blocking wait operations.
#[path = "notify/notify.rs"]
mod source;
// Generation and waker state protected by the shared inner lock.
mod wait_state;

pub(crate) use self::notified::Notified;
pub(crate) use self::source::Notify;

/// Maximum number of asynchronous waiters retained by one notification source.
///
/// Additional registrations fail with
/// [`crate::report::ObservationError::WaiterCapacityExceeded`].
pub(crate) const ASYNC_WAITER_CAPACITY: usize = 64;

/// Recovers a poisoned internal synchronization lock without exposing poison.
///
/// # Type Parameters
///
/// - `T`: The data protected by the synchronization lock.
///
/// # Parameters
///
/// - `mutex`: The synchronization lock to acquire.
///
/// # Returns
///
/// A guard borrowing `mutex`; if the lock is poisoned, the guard exposes its
/// inner value after recovering the poisoned state.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

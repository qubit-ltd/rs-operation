// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared, single-consumer result and repeatable settlement state.

use std::sync::Mutex;

// Future that consumes an asynchronously observed result.
mod result_future;
// Ownership state of the single typed business result.
mod result_slot;
// Facts serialized with the typed result and terminal settlement snapshot.
#[path = "state.rs"]
mod shared_state;
// Shared result cell publication and consumption operations.
mod state_cell;

pub(crate) use result_future::ResultFuture;
use result_slot::ResultSlot;
use shared_state::State;
pub(crate) use state_cell::StateCell;

/// Recovers a poisoned internal synchronization lock without exposing poison.
///
/// # Parameters
///
/// - `mutex`: The synchronization lock to acquire.
///
/// # Type Parameters
///
/// - `T`: The type of value protected by the mutex.
///
/// # Returns
///
/// A guard borrowing `mutex`; if the lock is poisoned, the guard exposes its
/// inner value after recovering the poisoned state.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

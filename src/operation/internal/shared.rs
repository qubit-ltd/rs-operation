// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Synchronization shared by operation handles and observers.

use std::sync::Condvar;
use std::sync::Mutex;

use super::State;
use crate::context::OperationContext;

/// Synchronizes a single operation's result, settlement, and waiters.
///
/// # Type Parameters
///
/// * `T` is the operation's successful result type.
/// * `E` is the operation's error type.
pub(crate) struct Shared<T, E> {
    /// Mutable operation state protected against concurrent observers.
    pub(crate) state: Mutex<State<T, E>>,
    /// Wakes blocking observers when either result or settlement changes.
    pub(crate) changed: Condvar,
    /// Context shared by the operation and its observers.
    pub(crate) context: OperationContext,
}

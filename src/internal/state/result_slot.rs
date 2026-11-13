// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The ownership states for a single business result.

use crate::report::OperationOutcome;

/// Tracks whether a result is pending, available, consumed, or discarded.
///
/// # Type Parameters
///
/// - `T` is the successful result payload type.
/// - `E` is the failure payload type.
pub(super) enum ResultSlot<T, E> {
    /// No result or accepted stop has been published.
    Pending,
    /// A stable outcome is available to one consumer.
    Ready(#[doc = "The typed outcome published for consumption."] OperationOutcome<T, E>),
    /// The consumer has already taken the outcome.
    Taken,
    /// The supervisor discarded the typed outcome but retains lifecycle state.
    Discarded,
}

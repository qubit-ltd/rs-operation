// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! One-shot delivery states for an operation outcome.

use crate::v02::report::OperationOutcome;

/// Tracks whether an operation outcome is pending, available, consumed, or
/// discarded.
///
/// # Type Parameters
///
/// * `T` is the successful outcome value.
/// * `E` is the failure value.
pub(in crate::v02::internal::state) enum ResultSlot<T, E> {
    /// No outcome has been published yet.
    Pending,
    /// The published outcome remains available to one consumer.
    Ready(
        /// Outcome value available to the next consumer.
        OperationOutcome<T, E>,
    ),
    /// The available outcome has already been taken.
    Taken,
    /// Delivery was discarded, so a later publication is rejected.
    Discarded,
}

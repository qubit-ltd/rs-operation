// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::CleanupFailure;
use crate::v02::report::TransferReceipt;

/// Reports whether a cleanup policy released, transferred or retained a
/// resource.
///
/// A successful release is explicitly confirmed. A transfer is successful only
/// when the receiver issued a receipt. On failure, the original resource stays
/// in this value so its owner can retry or recover it.
///
/// # Type Parameters
///
/// - `T`: The resource value returned when cleanup fails.
///
/// # Examples
///
/// ```
/// use qubit_operation::CleanupOutcome;
///
/// fn inspect(outcome: CleanupOutcome<String>) {
///     match outcome {
///         CleanupOutcome::Released => {}
///         CleanupOutcome::Transferred(receipt) => {
///             let _destination = receipt.target_scope();
///         }
///         CleanupOutcome::Failed { failure, resource } => {
///             let _retry_inputs = (failure, resource);
///         }
///     }
/// }
/// ```
#[must_use = "cleanup outcome carries resource ownership or transfer evidence"]
pub enum CleanupOutcome<T> {
    /// Cleanup was confirmed and the resource can be released.
    Released,
    /// The receiving scope accepted ownership and issued a transfer receipt.
    Transferred(
        /// Receipt proving the receiver accepted the resource ownership.
        TransferReceipt,
    ),
    /// Cleanup failed, so both its diagnostic and the original resource return.
    Failed {
        /// Diagnostic describing the cleanup failure.
        failure: CleanupFailure,
        /// Resource retained for retry or recovery.
        resource: T,
    },
}

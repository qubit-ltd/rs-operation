// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::super::TransferReceipt;
use super::CleanupFailure;
use super::RecoveryOwner;

/// Terminal fact returned by a cleanup action.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::CleanupOutcome;
///
/// let outcome = CleanupOutcome::Cleaned;
/// assert!(matches!(outcome, CleanupOutcome::Cleaned));
/// ```
#[derive(Debug)]
pub enum CleanupOutcome {
    /// Cleanup completed and release was confirmed.
    Cleaned,
    /// Another owner accepted responsibility, with explicit receipt.
    Transferred(
        /// Receipt identifying the owner that accepted responsibility.
        TransferReceipt,
    ),
    /// Cleanup failed while retaining an owned recovery payload.
    FailedWithRecovery(
        /// Bounded facts describing the cleanup failure.
        CleanupFailure,
        /// Opaque payload retained for recovery by the matching key.
        RecoveryOwner,
    ),
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Result of asking an operation to stop.
///
/// # Examples
///
/// Distinguish a newly accepted request from a repeated or late request:
///
/// ```
/// use qubit_operation::StopRequestStatus;
///
/// fn was_newly_accepted(status: StopRequestStatus) -> bool {
///     matches!(status, StopRequestStatus::Accepted)
/// }
///
/// assert!(was_newly_accepted(StopRequestStatus::Accepted));
/// assert!(!was_newly_accepted(StopRequestStatus::AlreadyRequested));
/// assert!(!was_newly_accepted(StopRequestStatus::AlreadyFinished));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopRequestStatus {
    /// The stop request was newly accepted.
    Accepted,
    /// A stop request was already recorded.
    AlreadyRequested,
    /// The operation had already finished.
    AlreadyFinished,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Reasons a cleanup receiver can reject a resource transfer.
///
/// Rejection leaves the submitted cleanup job with its sender so it can be
/// retried or retained by the caller.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupTransferError {
    /// The receiving scope no longer accepts transferred cleanup jobs.
    Closed,
    /// The receiving scope has no free cleanup reservation.
    Full,
    /// The transfer receipt belongs to a different manager.
    WrongManager,
    /// The source and receiving scopes are identical.
    SameScope,
    /// The receiver already contains this resource identity.
    Duplicate,
}

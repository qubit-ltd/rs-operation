// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Reason a cleanup dispatch reservation could not be created.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupQueueError {
    /// The bounded dispatch queue has no free reservation slot.
    Full,
    /// The cleanup dispatcher no longer accepts reservations.
    Closed,
}

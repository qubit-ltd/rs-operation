// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Configuration errors returned when creating a cleanup dispatcher.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CleanupDispatcherError {
    /// The dispatcher requires a positive bound for resource reservations.
    ZeroCapacity,
}

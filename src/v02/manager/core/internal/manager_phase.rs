// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Lifecycle phase controlling manager admission and shutdown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManagerPhase {
    /// Accepting new root and child operations.
    Open,
    /// Rejecting roots while allowing existing scopes to add descendants.
    Draining,
    /// Rejecting submissions and requesting active scopes to stop.
    Cancelling,
    /// All tracked scopes and resources have settled and executors are closed.
    Closed,
}

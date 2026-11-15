// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Tracks an operation child from adapter reservation through activation or
/// rollback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum StartState {
    /// Adapter has reserved the child but has not committed its start.
    Reserved,
    /// Adapter committed the child start but has not activated execution.
    Committed,
    /// Child execution has been activated.
    Activated,
    /// Child start was rolled back or rejected after a stop.
    Abandoned,
}

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Tracks whether a registered resource can be used or is awaiting cleanup.
///
/// The registry advances this state as use is closed, cleanup is driven, and
/// ownership is released or transferred. The state is internal to resource
/// coordination and is not an independent synchronization primitive.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::resource) enum EntryPhase {
    /// Accepts new use leases and has not begun cleanup.
    Open,
    /// Rejects new use leases and can become ready for cleanup.
    Closed,
    /// Its cleanup owner has been taken for a cleanup attempt.
    Cleaning,
    /// Cleanup responsibility moved to another owner; dependents may remain
    /// blocked until release is confirmed.
    Transferred,
    /// Release was confirmed and the registry slot can be reused.
    Released,
}

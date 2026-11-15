// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::super::CapacityPermit;
use super::super::PermitBundle;

/// Tracks permits and completion signals for one production operation lease.
///
/// The lease releases execution and outcome capacity at their respective
/// completion points, while child capacity remains held until both settlement
/// and outcome delivery have occurred.
pub(super) struct LeaseState {
    /// Root execution slot, released when settlement finishes.
    pub(super) active_root: Option<CapacityPermit>,
    /// Hierarchical child slots, released after both settlement and delivery.
    pub(super) child_path: Option<PermitBundle>,
    /// Outcome slot, released when the caller observes the outcome.
    pub(super) outcome: Option<CapacityPermit>,
    /// Whether the operation's resource and child state has settled.
    pub(super) settled: bool,
    /// Whether its outcome has been delivered to the caller.
    pub(super) delivered: bool,
}

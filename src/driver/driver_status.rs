// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Observable state of an external runtime lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverStatus {
    /// The external runtime still owns its execution lease.
    Active,
    /// The runtime was lost before release was confirmed.
    DriverLost,
    /// The adapter confirmed task completion and resource release.
    Released,
}

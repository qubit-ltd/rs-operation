// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Categories of failure reported by an internal execution driver.

/// Identifies why a driver could not accept an operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DriverError {
    /// No suitable driver is currently available.
    Unavailable,
    /// The selected driver has stopped accepting work.
    Closed,
    /// The selected driver has no remaining capacity.
    Capacity,
}

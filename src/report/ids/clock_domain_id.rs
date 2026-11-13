// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Identity of one monotonic clock domain.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::ClockDomainId;
///
/// let domain = ClockDomainId(42);
/// assert_eq!(domain.0, 42);
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClockDomainId(
    /// Distinguishes this clock's tick sequence from ticks produced by other
    /// clocks.
    ///
    /// Values from different domains must not be compared as timestamps.
    pub u64,
);

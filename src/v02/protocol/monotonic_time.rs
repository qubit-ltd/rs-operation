// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::clock_domain_id::ClockDomainId;

/// A monotonic tick value interpreted only within its clock domain.
///
/// Tick values from different domains cannot be compared meaningfully.
///
/// # Examples
///
/// ```
/// use qubit_operation::{ClockDomainId, MonotonicTime};
///
/// let domain = ClockDomainId::new_unique();
/// let time = MonotonicTime::new(domain, 42);
/// assert_eq!(time.domain(), domain);
/// assert_eq!(time.ticks(), 42);
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct MonotonicTime {
    /// Identity of the clock whose tick scale and origin interpret `ticks`.
    domain: ClockDomainId,
    /// Nondecreasing tick count within `domain`.
    ticks: u64,
}

impl MonotonicTime {
    /// Creates a time point from a clock domain and its tick count.
    ///
    /// # Parameters
    ///
    /// - `domain`: clock domain that defines the tick scale and origin.
    /// - `ticks`: tick count within that domain.
    #[inline]
    pub const fn new(domain: ClockDomainId, ticks: u64) -> Self {
        Self { domain, ticks }
    }

    /// Returns the clock domain that interprets this time point's ticks.
    #[inline]
    pub const fn domain(self) -> ClockDomainId {
        self.domain
    }

    /// Returns the tick count within this time point's clock domain.
    #[must_use]
    #[inline]
    pub const fn ticks(self) -> u64 {
        self.ticks
    }
}

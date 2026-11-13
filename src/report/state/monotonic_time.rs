// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::super::ClockDomainId;

/// Monotonic time point; only ticks from the same domain can be compared.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::{ClockDomainId, MonotonicTime};
///
/// let earlier = MonotonicTime::new(ClockDomainId(1), 4);
/// let later = MonotonicTime::new(ClockDomainId(1), 9);
/// assert_eq!(later.checked_duration_since(earlier), Some(5));
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MonotonicTime {
    /// Clock source identity, which defines the comparison domain.
    pub domain: ClockDomainId,
    /// Domain-local monotonically increasing tick count.
    pub ticks: u64,
}

impl MonotonicTime {
    /// Construct a domain-local monotonic time.
    ///
    /// # Parameters
    ///
    /// - `domain`: identifies the clock source; values from distinct domains
    ///   are incomparable.
    /// - `ticks`: the nondecreasing tick value reported by that clock.
    ///
    /// # Returns
    ///
    /// A time point containing the supplied domain and tick count.
    #[inline]
    pub const fn new(domain: ClockDomainId, ticks: u64) -> Self {
        Self { domain, ticks }
    }

    /// Return elapsed ticks only when both values share a domain and order.
    ///
    /// # Parameters
    ///
    /// - `earlier`: the earlier time point to subtract from `self`.
    ///
    /// # Returns
    ///
    /// `Some(ticks)` when both points share a clock domain and subtraction does
    /// not underflow; `None` for different domains or reverse order.
    #[must_use]
    #[inline]
    pub const fn checked_duration_since(self, earlier: Self) -> Option<u64> {
        if self.domain.0 != earlier.domain.0 {
            None
        } else {
            self.ticks.checked_sub(earlier.ticks)
        }
    }
}

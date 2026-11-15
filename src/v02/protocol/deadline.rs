// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::clock_domain_mismatch::ClockDomainMismatch;
use super::monotonic_time::MonotonicTime;

/// A deadline on a monotonic clock.
///
/// A deadline belongs to the domain of its time point. Combining deadlines from
/// different domains fails because their ticks cannot be compared.
///
/// # Examples
///
/// ```
/// use qubit_operation::{ClockDomainId, Deadline, MonotonicTime};
///
/// let domain = ClockDomainId::new_unique();
/// let earlier = Deadline::at(MonotonicTime::new(domain, 10));
/// let later = Deadline::at(MonotonicTime::new(domain, 20));
/// assert_eq!(earlier.earliest(later).unwrap().time_point().ticks(), 10);
/// ```
#[must_use]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Deadline {
    /// Monotonic instant, including its clock domain, at which the deadline
    /// occurs.
    at: MonotonicTime,
}

impl Deadline {
    /// Creates a deadline at the specified monotonic time point.
    ///
    /// # Parameters
    ///
    /// - `at`: time point at which the deadline occurs.
    ///
    /// # Returns
    ///
    /// A deadline bound to the clock domain and tick count in `at`.
    #[inline]
    pub const fn at(at: MonotonicTime) -> Self {
        Self { at }
    }

    /// Returns the time point at which this deadline occurs.
    ///
    /// # Parameters
    ///
    /// - `self`: Deadline whose time point is returned.
    ///
    /// # Returns
    ///
    /// The deadline's monotonic time point, including its clock domain.
    #[inline]
    pub const fn time_point(self) -> MonotonicTime {
        self.at
    }

    /// Returns the earlier of two deadlines in the same clock domain.
    ///
    /// # Parameters
    ///
    /// - `self`: Deadline that is compared with `other`.
    /// - `other`: deadline to compare with this one.
    ///
    /// # Returns
    ///
    /// The deadline with fewer ticks in the shared domain.
    ///
    /// # Errors
    ///
    /// Returns [`ClockDomainMismatch`] when the deadlines use different clock
    /// domains.
    pub fn earliest(self, other: Self) -> Result<Self, ClockDomainMismatch> {
        if self.at.domain() != other.at.domain() {
            return Err(ClockDomainMismatch {
                left: self.at.domain(),
                right: other.at.domain(),
            });
        }
        Ok(if self.at.ticks() <= other.at.ticks() {
            self
        } else {
            other
        })
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::cmp::Ordering;

use super::ClockDomainMismatch;
use crate::report::MonotonicTime;

/// Absolute deadline in one monotonic clock domain.
///
/// # Examples
///
/// ```
/// use qubit_operation::context::Deadline;
/// use qubit_operation::report::{ClockDomainId, MonotonicTime};
///
/// let domain = ClockDomainId(1);
/// let earlier = Deadline::at(MonotonicTime::new(domain, 10));
/// let later = Deadline::at(MonotonicTime::new(domain, 20));
/// assert_eq!(earlier.compare(later).unwrap(), core::cmp::Ordering::Less);
/// assert_eq!(earlier.earliest(later).unwrap(), earlier);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Deadline {
    /// Absolute monotonic instant that bounds the operation.
    pub(super) at: MonotonicTime,
}

impl Deadline {
    /// Constructs a deadline at a domain-local monotonic instant.
    ///
    /// # Parameters
    ///
    /// * `at` - Monotonic instant that defines the deadline and its clock
    ///   domain.
    ///
    /// # Returns
    ///
    /// A deadline at `at`.
    #[must_use]
    #[inline]
    pub const fn at(at: MonotonicTime) -> Self {
        Self { at }
    }

    /// Returns the absolute time point and its clock domain.
    ///
    /// # Returns
    ///
    /// The monotonic instant that defines this deadline.
    #[inline]
    pub const fn time_point(self) -> MonotonicTime {
        self.at
    }

    /// Compares deadlines, rejecting instants from different clock domains.
    ///
    /// # Parameters
    ///
    /// * `other` - Deadline to compare with this one.
    ///
    /// # Returns
    ///
    /// Their ordering when both deadlines use the same clock domain.
    ///
    /// # Errors
    ///
    /// Returns [`ClockDomainMismatch`] when the deadlines use different clocks.
    pub fn compare(self, other: Self) -> Result<Ordering, ClockDomainMismatch> {
        if self.at.domain != other.at.domain {
            return Err(ClockDomainMismatch {
                left: self.at.domain,
                right: other.at.domain,
            });
        }
        Ok(self.at.ticks.cmp(&other.at.ticks))
    }

    /// Returns the earlier deadline when both use the same clock domain.
    ///
    /// # Parameters
    ///
    /// * `other` - Deadline to compare with this one.
    ///
    /// # Returns
    ///
    /// The deadline with the smaller monotonic tick value.
    ///
    /// # Errors
    ///
    /// Returns [`ClockDomainMismatch`] when the deadlines use different clocks.
    pub fn earliest(self, other: Self) -> Result<Self, ClockDomainMismatch> {
        if self.compare(other)? == Ordering::Greater {
            Ok(other)
        } else {
            Ok(self)
        }
    }
}

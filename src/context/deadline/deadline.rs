// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::cmp::Ordering;

use crate::report::MonotonicTime;

use super::ClockDomainMismatch;

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
    pub(super) at: MonotonicTime,
}

impl Deadline {
    /// Construct a deadline at a domain-local monotonic instant.
    #[inline]
    pub const fn at(at: MonotonicTime) -> Self {
        Self { at }
    }

    /// Return the absolute time point and its clock domain.
    #[inline]
    pub const fn time_point(self) -> MonotonicTime {
        self.at
    }

    /// Compare deadlines; cross-domain comparisons are rejected.
    pub fn compare(self, other: Self) -> Result<Ordering, ClockDomainMismatch> {
        if self.at.domain != other.at.domain {
            return Err(ClockDomainMismatch {
                left: self.at.domain,
                right: other.at.domain,
            });
        }
        Ok(self.at.ticks.cmp(&other.at.ticks))
    }

    /// Return the earlier of two deadlines in the same clock domain.
    pub fn earliest(self, other: Self) -> Result<Self, ClockDomainMismatch> {
        if self.compare(other)? == Ordering::Greater {
            Ok(other)
        } else {
            Ok(self)
        }
    }
}

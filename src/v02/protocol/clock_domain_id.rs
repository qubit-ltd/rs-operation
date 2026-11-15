// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

static NEXT_CLOCK_DOMAIN: AtomicU64 = AtomicU64::new(1);

/// Opaque identity for one monotonic clock domain.
///
/// # Examples
///
/// ```
/// use qubit_operation::ClockDomainId;
///
/// let first = ClockDomainId::new_unique();
/// let second = ClockDomainId::new_unique();
/// assert_ne!(first, second);
/// ```
#[must_use = "retain the identity to distinguish clock domains"]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClockDomainId(
    /// Numeric identity used to distinguish clock domains.
    u128,
);

impl ClockDomainId {
    /// Creates a process-unique clock-domain identity for an external clock.
    ///
    /// # Returns
    ///
    /// A fresh identity that can distinguish this clock from other domains.
    #[inline]
    pub fn new_unique() -> Self {
        Self(u128::from(NEXT_CLOCK_DOMAIN.fetch_add(1, Ordering::Relaxed)))
    }

    /// Returns the underlying numeric identity for internal comparison.
    #[allow(dead_code)]
    pub(crate) const fn raw(self) -> u128 {
        self.0
    }
}

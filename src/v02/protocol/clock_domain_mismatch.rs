// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;

use super::clock_domain_id::ClockDomainId;

/// Error indicating that two monotonic times come from different clock domains.
///
/// Comparing ticks from distinct domains is invalid because each domain has its
/// own origin and progression.
///
/// # Examples
///
/// ```
/// use qubit_operation::{ClockDomainId, ClockDomainMismatch};
///
/// let mismatch = ClockDomainMismatch {
///     left: ClockDomainId::new_unique(),
///     right: ClockDomainId::new_unique(),
/// };
/// assert!(mismatch.to_string().contains("clock domains differ"));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClockDomainMismatch {
    /// Clock domain of the first time point in the rejected comparison.
    pub left: ClockDomainId,
    /// Clock domain of the second time point in the rejected comparison.
    pub right: ClockDomainId,
}

impl fmt::Display for ClockDomainMismatch {
    /// Formats the two clock domains that could not be compared.
    ///
    /// # Parameters
    ///
    /// - `self`: Mismatch whose clock domains are shown.
    /// - `f`: Formatter receiving the diagnostic text.
    ///
    /// # Returns
    ///
    /// The formatting result from writing both domain identifiers.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "clock domains differ: {:?} and {:?}", self.left, self.right)
    }
}
impl std::error::Error for ClockDomainMismatch {}

#[cfg(test)]
mod tests {
    use super::ClockDomainId;
    use super::ClockDomainMismatch;

    #[test]
    fn test_clock_domain_mismatch_identifies_both_domains() {
        let mismatch = ClockDomainMismatch {
            left: ClockDomainId::new_unique(),
            right: ClockDomainId::new_unique(),
        };
        let message = mismatch.to_string();

        assert!(message.starts_with("clock domains differ: ClockDomainId("));
        assert!(message.contains(" and ClockDomainId("));
        assert!(std::error::Error::source(&mismatch).is_none());
    }
}

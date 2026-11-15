// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

use super::ResourceLabel;

/// Capacity failure that identifies the exhausted pool without exposing its
/// internal `qubit-budget` accounting state.
#[must_use]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapacityError {
    /// Pool whose permit request could not be satisfied.
    pub(in crate::v02::manager::capacity) resource: ResourceLabel,
}

impl fmt::Display for CapacityError {
    /// Formats the exhausted capacity label for diagnostics.
    ///
    /// # Parameters
    ///
    /// - `f`: formatter that receives the diagnostic text.
    ///
    /// # Returns
    ///
    /// `Ok(())` when formatting succeeds, or the formatter's write error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "capacity exhausted for {:?}", self.resource)
    }
}

impl Error for CapacityError {}

#[cfg(test)]
mod tests {
    use super::CapacityError;
    use super::ResourceLabel;

    #[test]
    fn test_display_names_the_exhausted_capacity_pool() {
        let error = CapacityError {
            resource: ResourceLabel::Named("active operations"),
        };

        assert_eq!(
            error.to_string(),
            r#"capacity exhausted for Named("active operations")"#
        );
        assert!(std::error::Error::source(&error).is_none());
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Reason a resource scope could not reserve more resources.
///
/// # Examples
///
/// Handle a full scope separately from a scope that has already ended:
///
/// ```
/// use qubit_operation::ResourceCapacityError;
///
/// fn may_retry(error: ResourceCapacityError) -> bool {
///     matches!(error, ResourceCapacityError::CapacityExceeded)
/// }
///
/// assert!(may_retry(ResourceCapacityError::CapacityExceeded));
/// assert!(!may_retry(ResourceCapacityError::ScopeEnded));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceCapacityError {
    /// The resource owner scope has ended.
    ScopeEnded,
    /// The scope has been stopped.
    Stopped,
    /// The scope has reached its resource capacity.
    CapacityExceeded,
}

impl fmt::Display for ResourceCapacityError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// - `self`: Capacity failure reason being formatted.
    /// - `f`: Formatter receiving the display text.
    ///
    /// # Returns
    ///
    /// A formatting result indicating whether writing succeeded.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for ResourceCapacityError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::ResourceCapacityError;

    #[test]
    fn test_display_is_debug_and_error_has_no_source() {
        let error = ResourceCapacityError::ScopeEnded;
        assert_eq!(error.to_string(), format!("{error:?}"));
        assert!(Error::source(&error).is_none());
    }
}

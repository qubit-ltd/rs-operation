// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Reason a resource scope could not be created or used.
///
/// # Examples
///
/// A full parent may be retried later, while invalid options need correction:
///
/// ```
/// use qubit_operation::ResourceScopeError;
///
/// fn can_retry(error: ResourceScopeError) -> bool {
///     matches!(error, ResourceScopeError::CapacityExceeded)
/// }
///
/// assert!(can_retry(ResourceScopeError::CapacityExceeded));
/// assert!(!can_retry(ResourceScopeError::InvalidOptions));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceScopeError {
    /// The parent scope has ended.
    ScopeEnded,
    /// The parent scope has been stopped.
    Stopped,
    /// Requested options violate scope constraints.
    InvalidOptions,
    /// The parent reached its child-scope capacity.
    CapacityExceeded,
}

impl fmt::Display for ResourceScopeError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// - `self`: Resource-scope failure reason being formatted.
    /// - `f`: Formatter receiving the display text.
    ///
    /// # Returns
    ///
    /// A formatting result indicating whether writing succeeded.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for ResourceScopeError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::ResourceScopeError;

    #[test]
    fn test_display_is_debug_and_error_has_no_source() {
        let error = ResourceScopeError::ScopeEnded;
        assert_eq!(error.to_string(), format!("{error:?}"));
        assert!(Error::source(&error).is_none());
    }
}

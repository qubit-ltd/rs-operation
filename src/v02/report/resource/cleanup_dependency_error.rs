// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Reason a cleanup dependency could not be registered.
///
/// # Examples
///
/// Match the error to decide whether the dependency request can be corrected:
///
/// ```
/// use qubit_operation::CleanupDependencyError;
///
/// fn can_retry(error: CleanupDependencyError) -> bool {
///     matches!(error, CleanupDependencyError::Cycle)
/// }
///
/// assert!(can_retry(CleanupDependencyError::Cycle));
/// assert!(!can_retry(CleanupDependencyError::CleanupStarted));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupDependencyError {
    /// The resource identity is not registered with this manager.
    UnknownResource,
    /// The resource belongs to another manager.
    WrongManager,
    /// The resource belongs to another scope.
    WrongScope,
    /// A resource cannot depend on its own cleanup.
    SelfDependency,
    /// Adding the edge would introduce a cleanup cycle.
    Cycle,
    /// Cleanup has already begun and dependencies are frozen.
    CleanupStarted,
}

impl fmt::Display for CleanupDependencyError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// - `self`: Dependency registration failure being formatted.
    /// - `f`: Formatter receiving the display text.
    ///
    /// # Returns
    ///
    /// A formatting result indicating whether writing succeeded.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for CleanupDependencyError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::CleanupDependencyError;

    #[test]
    fn test_display_is_debug_and_error_has_no_source() {
        let error = CleanupDependencyError::UnknownResource;
        assert_eq!(error.to_string(), format!("{error:?}"));
        assert!(Error::source(&error).is_none());
    }
}

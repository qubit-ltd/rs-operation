// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;

/// Reason ownership of a resource group could not be transferred.
///
/// # Examples
///
/// A capacity rejection may succeed after capacity becomes available, while an
/// incomplete dependency group must be corrected by the caller:
///
/// ```
/// use qubit_operation::ResourceTransferError;
///
/// fn is_capacity_rejection(error: ResourceTransferError) -> bool {
///     matches!(error, ResourceTransferError::CapacityExceeded)
/// }
///
/// assert!(is_capacity_rejection(ResourceTransferError::CapacityExceeded));
/// assert!(!is_capacity_rejection(ResourceTransferError::IncompleteDependencyGroup));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceTransferError {
    /// The resource identity is not registered with this manager.
    UnknownResource,
    /// The resource belongs to another manager.
    WrongManager,
    /// Source and target scopes are identical.
    SameScope,
    /// The target scope has ended.
    TargetEnded,
    /// The target scope has been stopped.
    TargetStopped,
    /// The target scope has no remaining resource capacity.
    CapacityExceeded,
    /// Cleanup already began for one of the resources.
    CleanupStarted,
    /// The request omitted a member of a dependency-connected group.
    IncompleteDependencyGroup,
}

impl fmt::Display for ResourceTransferError {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// - `self`: Transfer rejection reason being formatted.
    /// - `f`: Formatter receiving the display text.
    ///
    /// # Returns
    ///
    /// A formatting result indicating whether writing succeeded.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for ResourceTransferError {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::ResourceTransferError;

    #[test]
    fn test_display_is_debug_and_error_has_no_source() {
        let error = ResourceTransferError::UnknownResource;
        assert_eq!(error.to_string(), format!("{error:?}"));
        assert!(Error::source(&error).is_none());
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Category of failure returned by resource-management operations.
///
/// # Examples
///
/// Distinguish a capacity limit from a scope lifecycle failure:
///
/// ```
/// use qubit_operation::ResourceManageErrorKind;
///
/// fn is_capacity_failure(error: ResourceManageErrorKind) -> bool {
///     matches!(error, ResourceManageErrorKind::CapacityExceeded)
/// }
///
/// assert!(is_capacity_failure(ResourceManageErrorKind::CapacityExceeded));
/// assert!(!is_capacity_failure(ResourceManageErrorKind::ScopeEnded));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceManageErrorKind {
    /// The scope has reached its resource capacity.
    CapacityExceeded,
    /// The owning scope has ended.
    ScopeEnded,
    /// The owning scope has been stopped.
    Stopped,
}

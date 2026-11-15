// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Reason a resource lookup could not produce a reference.
///
/// # Examples
///
/// A stale key cannot be reused, while a scope-ending error is a lifecycle
/// result for the existing lookup attempt:
///
/// ```
/// use qubit_operation::ResourceLookupError;
///
/// fn key_is_stale(error: ResourceLookupError) -> bool {
///     matches!(error, ResourceLookupError::StaleKey)
/// }
///
/// assert!(key_is_stale(ResourceLookupError::StaleKey));
/// assert!(!key_is_stale(ResourceLookupError::ScopeEnded));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceLookupError {
    /// The owning scope has ended.
    ScopeEnded,
    /// The owning scope has been stopped.
    Stopped,
    /// The key identifies a resource owned by another scope.
    WrongScope,
    /// The key generation no longer matches the resource slot.
    StaleKey,
}

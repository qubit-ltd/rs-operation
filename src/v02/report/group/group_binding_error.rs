// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Errors returned when a manager view cannot be bound to a service group.

use std::error::Error;
use std::fmt;

/// Describes why a manager view cannot bind to a service group.
///
/// The error preserves the ownership and lifecycle constraints checked during
/// binding so callers can distinguish an invalid group from an ended scope.
///
/// # Examples
///
/// ```
/// use qubit_operation::GroupBindingError;
///
/// fn report_binding(result: Result<(), GroupBindingError>) {
///     if let Err(error) = result {
///         eprintln!("manager view could not be bound: {error}");
///     }
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupBindingError {
    /// The group is owned by a different manager than the view.
    WrongManager,
    /// The group has stopped accepting operations and cannot be bound.
    Closed,
    /// The parent operation scope ended before the binding was committed.
    ScopeEnded,
    /// Adding the group would exceed the configured distinct-group limit.
    CapacityExceeded,
}

impl fmt::Display for GroupBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for GroupBindingError {}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Mutable registration state shared by cloned operation scopes.

use super::super::OperationScope;
use super::Child;
use crate::report::StopReason;

/// Registration gate, cancellation state, and accepted child work.
pub(in crate::scope) struct State {
    /// Whether new child reservations and child scopes are accepted.
    pub(in crate::scope) accepting: bool,
    /// First cancellation reason observed by the scope.
    pub(in crate::scope) stop: Option<StopReason>,
    /// Child operation registrations owned by this scope.
    pub(in crate::scope) children: Vec<Child>,
    /// Nested scopes whose work joins this scope.
    pub(in crate::scope) child_scopes: Vec<OperationScope>,
    /// Reservations not yet committed to a child operation.
    pub(in crate::scope) reserved_children: usize,
}

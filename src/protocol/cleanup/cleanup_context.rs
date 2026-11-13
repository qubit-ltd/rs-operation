// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Cleanup-only authority; its deadline does not inherit business cancellation.
///
/// # Examples
///
/// ```
/// use qubit_operation::protocol::CleanupContext;
///
/// let context = CleanupContext { deadline: None };
/// assert!(context.deadline.is_none());
/// ```
#[derive(Clone, Copy, Debug)]
pub struct CleanupContext {
    /// Optional cleanup deadline in the configured clock domain.
    pub deadline: Option<crate::report::MonotonicTime>,
}

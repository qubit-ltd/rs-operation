// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Failures returned by scope registration and resource operations.

/// A scope operation failed before it could complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeError {
    /// The scope no longer accepts new registrations.
    Closed,
    /// A configured child, resource, or effect limit was reached.
    LimitReached,
    /// The scope's bounded effect record storage is full.
    EffectLimitReached,
    /// A child reservation already has an attached operation.
    AlreadyAttached,
    /// A child reservation has no attached operation to commit.
    NotAttached,
    /// Cancellation was requested before the child could be committed.
    Cancelled,
    /// Child context options could not be combined with the parent context.
    InvalidContext,
}

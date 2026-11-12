// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Configuration passed when reserving a child operation.

use super::ChildRequirement;

/// Describes the result policy for a child before it is registered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChildOptions {
    /// Determines whether an unhandled child failure blocks the parent result.
    pub requirement: ChildRequirement,
}

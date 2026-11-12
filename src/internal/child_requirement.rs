// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The policy for propagating child operation failures.

/// Indicates whether an unhandled child failure blocks its parent's result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChildRequirement {
    /// The parent result depends on this child completing successfully.
    Required,
    /// A child failure does not by itself prevent the parent from succeeding.
    Optional,
}

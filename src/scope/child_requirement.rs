// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Failure requirements for registered child operations.

/// Whether a child failure affects its parent operation result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildRequirement {
    /// A failure contributes to the parent operation's business result.
    Required,
    /// A failure is reported but does not fail the parent operation.
    Optional,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Selects which parent admission rules apply while reserving an operation.
#[derive(Clone, Copy, Default)]
pub(crate) struct ScopeAdmissionPolicy {
    /// Allows an already claimed child to finish admission after manager drain.
    pub allow_claimed_after_close: bool,
    /// Reuses group slots already held by a parent scope.
    pub reuse_parent_groups: bool,
}

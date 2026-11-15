// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Per-scope capacity pools.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeCapacityKind {
    /// Descendant operation slots retained across this scope's ancestors.
    ChildOperations,
    /// Resource registrations retained across this scope's ancestors.
    Resources,
    /// Resource-scope instances retained across this scope's ancestors.
    ResourceScopes,
    /// Effect records retained across this scope's ancestors.
    EffectRecords,
}

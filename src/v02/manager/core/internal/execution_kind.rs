// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Selects how an accepted operation body is driven by its execution engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionKind {
    /// A body executed as a blocking synchronous closure.
    Sync,
    /// A body driven as a `Send` future.
    Async,
}

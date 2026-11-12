// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Parent behavior selected for required child failures.

/// Parent behavior after a required child reports failure.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ScopePolicy {
    /// Request cancellation as soon as a required child fails.
    #[default]
    FailFast,
    /// Collect child outcomes and let the caller decide how to handle failures.
    Collect,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Executor admission failure. A rejected submit preserves and returns its job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutorError {
    /// The bounded queue has no free admission slot.
    Full,
    /// The executor no longer accepts new work.
    Closed,
}

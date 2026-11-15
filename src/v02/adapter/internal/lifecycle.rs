// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::ChildPhase;

/// Shared state that prevents duplicate child lifecycle transitions.
pub(in crate::v02::adapter) struct Lifecycle {
    /// Current admission, commit, activation or abandonment phase.
    pub(super) phase: ChildPhase,
    /// Whether a stop request arrived before commit or activation.
    pub(super) stop_requested: bool,
}

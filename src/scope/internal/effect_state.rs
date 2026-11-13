// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded side-effect facts retained for settlement reporting.

use crate::report::EffectRecord;

/// Effect records shared by scopes in one operation tree.
#[derive(Default)]
pub(in crate::scope) struct EffectState {
    /// Maximum number of records retained.
    pub(in crate::scope) limit: usize,
    /// Caller-declared effects retained within the configured limit.
    pub(in crate::scope) records: Vec<EffectRecord>,
    /// Number of additional records rejected after reaching the limit.
    pub(in crate::scope) overflow: usize,
}

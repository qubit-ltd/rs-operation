// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::MonotonicTime;
use super::StopReason;

/// Immutable stop fact attached to a business result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StopReport {
    /// First stop cause accepted by the operation state.
    pub reason: StopReason,
    /// Clock-domain timestamp of acceptance, when the clock was available.
    pub accepted_at: Option<MonotonicTime>,
}

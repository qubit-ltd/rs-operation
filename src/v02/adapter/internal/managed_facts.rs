// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::task::Waker;

use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::ChildRelease;

/// Facts mirrored from a managed child and wakers awaiting their publication.
pub(in crate::v02::adapter) struct ManagedFacts {
    /// First published outcome for this child, if available.
    pub(super) outcome: Option<ChildOutcomeSummary>,
    /// First confirmed input-release report, if available.
    pub(super) release: Option<ChildRelease>,
    /// Tasks waiting for an outcome publication.
    pub(super) outcome_waiters: Vec<Waker>,
    /// Tasks waiting for input release.
    pub(super) release_waiters: Vec<Waker>,
    /// Tasks waiting for the core settlement report.
    pub(super) settlement_waiters: Vec<Waker>,
    /// Changes whenever settlement observers need to retry registration.
    pub(super) settlement_generation: u64,
}

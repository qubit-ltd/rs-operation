// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashSet;
use std::sync::Arc;

use crate::v02::report::CleanupFailure;
use crate::v02::report::ResourceCleanupReport;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceProgress;
use crate::v02::report::ResourceScopeReport;
use crate::v02::report::TransferReceipt;

/// Accumulates resource outcomes and observers for one scope settlement.
///
/// The enclosing [`Mutex`](std::sync::Mutex) protects these counters and
/// collections while resource cleanups start, complete, and report recovery.
pub(in crate::v02::resource::core) struct ScopeCompletionState {
    /// Expected number of resource completions after settlement is sealed.
    pub(in crate::v02::resource::core) expected: Option<usize>,
    /// Pending reservations released before sealing and subtracted from
    /// `expected`.
    pub(in crate::v02::resource::core) reservations_released_before_seal: usize,
    /// Number of resource cleanup operations that have started.
    pub(in crate::v02::resource::core) started: usize,
    /// Number of distinct resource completions already recorded.
    pub(in crate::v02::resource::core) completed: usize,
    /// Resource identities already completed, used to ignore duplicate reports.
    pub(in crate::v02::resource::core) seen: HashSet<ResourceId>,
    /// Confirmed resource cleanup evidence included in the settlement report.
    pub(in crate::v02::resource::core) resources: Vec<ResourceCleanupReport>,
    /// Cleanup failures collected for the settlement report.
    pub(in crate::v02::resource::core) failures: Vec<CleanupFailure>,
    /// Recovery handles retained for resources whose cleanup is unresolved.
    pub(in crate::v02::resource::core) recoveries: Vec<crate::v02::resource::recovery::RecoveryHandle>,
    /// Observer notified when recovery handles appear or reach a terminal
    /// state.
    pub(in crate::v02::resource::core) recovery_callback:
        Option<Arc<dyn Fn(crate::v02::resource::recovery::RecoveryHandle, bool) + Send + Sync + 'static>>,
    /// Transfer receipts retained in the report's bounded sample.
    pub(in crate::v02::resource::core) transfers: Vec<TransferReceipt>,
    /// Total transfer receipts observed, including receipts omitted from the
    /// sample.
    pub(in crate::v02::resource::core) transfers_total: usize,
    /// Whether more than 256 receipts were observed and omitted from the
    /// sample.
    pub(in crate::v02::resource::core) transfers_truncated: bool,
    /// One-shot callback invoked with the final report once expected work
    /// completes.
    pub(in crate::v02::resource::core) callback: Option<Box<dyn FnOnce(ResourceScopeReport) + Send + 'static>>,
    /// Latest monotonic progress state for the scope settlement.
    pub(in crate::v02::resource::core) progress: ResourceProgress,
    /// Observer notified whenever scope cleanup progress advances.
    pub(in crate::v02::resource::core) progress_callback: Option<Arc<dyn Fn(ResourceProgress) + Send + Sync + 'static>>,
}

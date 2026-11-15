// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::CleanupFailure;
use crate::v02::report::RecoveryHandle;
use crate::v02::report::ResourceCleanupReport;
use crate::v02::report::TransferReceipt;

/// Cleanup results accumulated for a resource scope.
///
/// # Examples
///
/// Construct and inspect an empty report before combining it with cleanup data:
///
/// ```
/// use qubit_operation::ResourceScopeReport;
///
/// let report = ResourceScopeReport {
///     resources: Vec::new().into(),
///     failures: Vec::new().into(),
///     recoveries: Vec::new().into(),
///     transfers: Vec::new().into(),
///     transfers_total: 0,
///     transfers_truncated: false,
/// };
/// assert_eq!(report.resources.len(), 0);
/// assert!(!report.transfers_truncated);
/// ```
#[derive(Clone, Debug)]
pub struct ResourceScopeReport {
    /// Retained per-resource cleanup evidence.
    pub resources: Arc<[ResourceCleanupReport]>,
    /// Retained cleanup failures.
    pub failures: Arc<[CleanupFailure]>,
    /// Recovery handles for resources needing later retry.
    pub recoveries: Arc<[RecoveryHandle]>,
    /// Receipts for resources transferred out of the scope.
    pub transfers: Arc<[TransferReceipt]>,
    /// Total transfers observed before truncation.
    pub transfers_total: usize,
    /// Whether the transfer details were truncated.
    pub transfers_truncated: bool,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Aggregate facts produced by recursive operation resource cleanup.

use crate::report::CleanupFailureSummary;
use crate::report::ResourcePhase;
use crate::report::TransferReceipt;

/// Summarizes cleanup collected from an operation scope and its descendants.
pub(crate) struct CleanupFacts {
    /// Aggregate resource phase after cleanup.
    pub(crate) phase: ResourcePhase,
    /// Resources that failed cleanup and require recovery.
    pub(crate) failures: Vec<CleanupFailureSummary>,
    /// Resources transferred to another owner.
    pub(crate) transfers: Vec<TransferReceipt>,
    /// Number of resources whose final state could not be confirmed.
    pub(crate) unconfirmed: usize,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::CleanupEvidence;
use crate::v02::report::ResourceId;

/// Cleanup evidence associated with one managed resource.
///
/// # Examples
///
/// Inspect the identity and evidence returned in a cleanup report:
///
/// ```
/// use qubit_operation::{
///     CleanupEvidence, ResourceCleanupReport, ResourceId,
/// };
///
/// fn cleanup_details(report: &ResourceCleanupReport) -> (ResourceId, CleanupEvidence) {
///     (report.resource_id, report.evidence)
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ResourceCleanupReport {
    /// Identity of the resource whose cleanup was observed.
    pub resource_id: ResourceId,
    /// Evidence that cleanup completed or was explicitly confirmed.
    pub evidence: CleanupEvidence,
}

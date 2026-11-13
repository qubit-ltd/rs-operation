// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use super::super::DiagnosticSummary;
use super::super::ResourceId;
use super::CleanupStage;

/// Cleanup failure with a bounded summary and optional shared typed source.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::CleanupFailure;
/// use qubit_operation::report::CleanupStage;
/// use qubit_operation::report::DiagnosticSummary;
/// use qubit_operation::report::OperationId;
/// use qubit_operation::report::ResourceId;
///
/// let failure = CleanupFailure {
///     resource_id: ResourceId::new(OperationId::new(7), 2, 3),
///     stage: CleanupStage::Releasing,
///     summary: DiagnosticSummary::new("release was not confirmed"),
///     source: None,
/// };
/// assert_eq!(failure.summary.as_str(), "release was not confirmed");
/// ```
#[derive(Clone)]
pub struct CleanupFailure {
    /// Resource whose cleanup failed.
    pub resource_id: ResourceId,
    /// Cleanup phase at failure.
    pub stage: CleanupStage,
    /// Sanitized bounded diagnostic text.
    pub summary: DiagnosticSummary,
    /// Typed source shared by `Arc`; arbitrary errors need not implement
    /// `Clone`.
    pub source: Option<Arc<dyn Error + Send + Sync>>,
}

impl fmt::Debug for CleanupFailure {
    /// Formats stable failure facts without exposing the typed source.
    ///
    /// # Parameters
    ///
    /// * `f` — formatter receiving the debug representation.
    ///
    /// # Returns
    ///
    /// The formatter's success or formatting error status.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CleanupFailure")
            .field("resource_id", &self.resource_id)
            .field("stage", &self.stage)
            .field("summary", &self.summary)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

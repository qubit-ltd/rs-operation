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

use crate::v02::report::DiagnosticSummary;
use crate::v02::report::OperationId;

/// Executor failure metadata, retaining the original error when available.
///
/// # Examples
///
/// Read bounded diagnostics and preserve the optional executor error:
///
/// ```
/// use qubit_operation::{ExecutionFailure, OperationId};
///
/// fn failure_details(failure: &ExecutionFailure) -> (OperationId, &str, Option<String>) {
///     (
///         failure.operation_id,
///         failure.summary.as_str(),
///         failure.source.as_ref().map(|source| source.to_string()),
///     )
/// }
/// ```
#[derive(Clone)]
pub struct ExecutionFailure {
    /// Identity of the operation that could not be executed.
    pub operation_id: OperationId,
    /// Bounded diagnostic suitable for reports.
    pub summary: DiagnosticSummary,
    /// Original executor error as `Some`, or `None` if the executor supplied no
    /// source.
    pub source: Option<Arc<dyn Error + Send + Sync>>,
}

impl fmt::Debug for ExecutionFailure {
    /// Formats this value for diagnostic inspection.
    ///
    /// # Parameters
    ///
    /// - `f`: Formatter receiving the executor failure fields.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExecutionFailure")
            .field("operation_id", &self.operation_id)
            .field("summary", &self.summary)
            .field("source", &self.source.as_ref().map(|e| e.to_string()))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::ExecutionFailure;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::OperationId;

    #[test]
    fn test_debug_includes_operation_summary_and_optional_source() {
        let failure = ExecutionFailure {
            operation_id: OperationId::from_raw(8),
            summary: DiagnosticSummary::new("worker failed"),
            source: Some(std::sync::Arc::new(std::io::Error::other("offline"))),
        };
        let debug = format!("{failure:?}");
        assert!(debug.contains("operation_id: OperationId(8)"));
        assert!(debug.contains("summary: DiagnosticSummary { text: \"worker failed\", truncated: false }"));
        assert!(debug.contains("source: Some(\"offline\")"));
    }
}

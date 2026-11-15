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

use crate::v02::report::CleanupStage;
use crate::v02::report::DiagnosticSummary;
use crate::v02::report::ResourceId;

/// Failure while cleaning one resource at a named protocol stage.
///
/// The bounded summary is always available; the original error is retained
/// separately when the resource hook supplied one.
///
/// # Examples
///
/// Format a cleanup failure and preserve its optional source for error
/// chaining:
///
/// ```
/// use qubit_operation::CleanupFailure;
/// use std::error::Error;
///
/// fn failure_details(
///     failure: &CleanupFailure,
/// ) -> (&str, Option<&(dyn Error + 'static)>) {
///     (failure.summary.as_str(), failure.source())
/// }
/// ```
#[must_use]
#[derive(Clone, Debug)]
pub struct CleanupFailure {
    /// Identity of the resource whose cleanup failed.
    pub resource_id: ResourceId,
    /// Cleanup stage that produced the failure.
    pub stage: CleanupStage,
    /// Bounded diagnostic suitable for storing in reports.
    pub summary: DiagnosticSummary,
    /// Original hook error as `Some`, or `None` if the hook supplied no source.
    pub source: Option<Arc<dyn Error + Send + Sync>>,
}

impl fmt::Display for CleanupFailure {
    /// Formats this error as a concise diagnostic for callers.
    ///
    /// # Parameters
    ///
    /// - `f`: Formatter receiving the cleanup failure diagnostic.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cleanup {:?} failed for {:?}: {}",
            self.stage, self.resource_id, self.summary
        )
    }
}

impl Error for CleanupFailure {
    /// Returns the underlying cause when this error wraps one.
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| source as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::CleanupFailure;
    use crate::v02::report::CleanupStage;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ResourceId;

    #[test]
    fn test_display_and_source_describe_a_cleanup_failure() {
        let failure = CleanupFailure {
            resource_id: ResourceId::new(ManagerId::from_raw(2), 5, 1),
            stage: CleanupStage::Drop,
            summary: DiagnosticSummary::new("close failed"),
            source: Some(std::sync::Arc::new(std::io::Error::other("offline"))),
        };
        assert_eq!(
            failure.to_string(),
            "cleanup Drop failed for ResourceId { manager: ManagerId(2), slot: 5, generation: 1 }: close failed"
        );
        assert_eq!(Error::source(&failure).unwrap().to_string(), "offline");
    }
}

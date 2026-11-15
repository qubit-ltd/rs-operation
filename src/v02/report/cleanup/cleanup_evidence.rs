// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Evidence available for a resource cleanup result.
///
/// These variants distinguish ordinary value drop from an explicit cleanup
/// confirmation.
///
/// # Examples
///
/// Handle each evidence source explicitly when presenting a cleanup result:
///
/// ```
/// use qubit_operation::CleanupEvidence;
///
/// fn evidence_label(evidence: CleanupEvidence) -> &'static str {
///     match evidence {
///         CleanupEvidence::DropCompleted => "value dropped",
///         CleanupEvidence::Confirmed => "cleanup confirmed",
///     }
/// }
///
/// assert_eq!(evidence_label(CleanupEvidence::Confirmed), "cleanup confirmed");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupEvidence {
    /// Dropping the resource value completed.
    DropCompleted,
    /// The cleanup protocol explicitly confirmed release.
    Confirmed,
}

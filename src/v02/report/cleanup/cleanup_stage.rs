// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Resource cleanup stage associated with a failure.
///
/// # Examples
///
/// Match a reported stage to decide which cleanup path needs attention:
///
/// ```
/// use qubit_operation::CleanupStage;
///
/// fn stage_name(stage: CleanupStage) -> &'static str {
///     match stage {
///         CleanupStage::Drop => "drop",
///         CleanupStage::AsyncRelease => "async release",
///         CleanupStage::Dependency => "dependency",
///         CleanupStage::Transfer => "transfer",
///     }
/// }
///
/// assert_eq!(stage_name(CleanupStage::Dependency), "dependency");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupStage {
    /// Dropping the managed value.
    Drop,
    /// Awaiting its asynchronous release hook.
    AsyncRelease,
    /// Waiting for a dependent resource.
    Dependency,
    /// Moving ownership to another scope.
    Transfer,
}

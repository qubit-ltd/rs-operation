// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Progress of resource cleanup after an operation body finishes.
///
/// # Examples
///
/// Describe the cleanup state reported for an operation:
///
/// ```
/// use qubit_operation::ResourceProgress;
///
/// fn progress_label(progress: ResourceProgress) -> &'static str {
///     match progress {
///         ResourceProgress::ScopeOpen => "scope open",
///         ResourceProgress::WaitingForUsers => "waiting for users",
///         ResourceProgress::Cleaning => "cleaning",
///         ResourceProgress::Unconfirmed => "unconfirmed",
///     }
/// }
///
/// assert_eq!(progress_label(ResourceProgress::WaitingForUsers), "waiting for users");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceProgress {
    /// The owner scope still accepts or retains resources.
    ScopeOpen,
    /// Cleanup waits for users to release references.
    WaitingForUsers,
    /// Cleanup actions are executing.
    Cleaning,
    /// Cleanup could not yet be confirmed.
    Unconfirmed,
}

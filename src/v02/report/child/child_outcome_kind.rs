// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Summary classification of a child operation's outcome.
///
/// The classification keeps application failures, stops, panics, and executor
/// failures separate while omitting their detailed diagnostics.
///
/// # Examples
///
/// Map each classification to a concise caller-facing label:
///
/// ```
/// use qubit_operation::ChildOutcomeKind;
///
/// fn label(kind: ChildOutcomeKind) -> &'static str {
///     match kind {
///         ChildOutcomeKind::Succeeded => "success",
///         ChildOutcomeKind::Failed => "application failure",
///         ChildOutcomeKind::Stopped => "stopped",
///         ChildOutcomeKind::Panicked => "body panic",
///         ChildOutcomeKind::ExecutionFailed => "executor failure",
///     }
/// }
///
/// assert_eq!(label(ChildOutcomeKind::ExecutionFailed), "executor failure");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildOutcomeKind {
    /// The child body returned successfully.
    Succeeded,
    /// The child body returned its application error.
    Failed,
    /// The child was stopped before successful completion.
    Stopped,
    /// The child body panicked.
    Panicked,
    /// The executor failed to run the child body.
    ExecutionFailed,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Execution state independent of result consumption and resource settlement.
///
/// # Examples
///
/// Distinguish queued, running, and finished operations in a snapshot:
///
/// ```
/// use qubit_operation::ExecutionPhase;
///
/// fn is_terminal(phase: ExecutionPhase) -> bool {
///     match phase {
///         ExecutionPhase::Queued | ExecutionPhase::Running => false,
///         ExecutionPhase::Finished => true,
///     }
/// }
///
/// assert!(is_terminal(ExecutionPhase::Finished));
/// assert!(!is_terminal(ExecutionPhase::Queued));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionPhase {
    /// The operation awaits executor capacity.
    Queued,
    /// The operation body is running.
    Running,
    /// The operation body has returned or stopped.
    Finished,
}

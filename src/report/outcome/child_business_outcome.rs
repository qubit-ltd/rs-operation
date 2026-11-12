// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Erased business result category for child observation.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::ChildBusinessOutcome;
///
/// let outcome = ChildBusinessOutcome::Stopped;
/// assert!(matches!(outcome, ChildBusinessOutcome::Stopped));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildBusinessOutcome {
    /// The child body and its required work completed successfully.
    Succeeded,
    /// The child body returned a business-domain error.
    Failed,
    /// The child stopped before determining a business result.
    Stopped,
    /// The child body panicked.
    Panicked,
}

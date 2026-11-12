// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Stop cause recorded by the supervisor; acceptance does not prove work has
/// stopped.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::StopReason;
///
/// let reason = StopReason::Timeout;
/// assert_eq!(reason, StopReason::Timeout);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopReason {
    /// Cancellation explicitly requested by the caller.
    UserCancelled,
    /// The operation deadline expired.
    Timeout,
    /// An ancestor scope requested stop.
    ParentStopped,
    /// Supervisor shutdown requested cancellation.
    SupervisorShutdown,
    /// A required child failure triggered fail-fast stop.
    RequiredChildFailed,
}

/// Caller-level stop request; the supervisor maps it to a lifecycle
/// [`StopReason`].
pub type CancelReason = StopReason;

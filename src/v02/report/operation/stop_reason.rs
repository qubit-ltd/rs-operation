// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Cause recorded when an operation is asked to stop.
///
/// # Examples
///
/// Map a recorded stop reason to the action that initiated it:
///
/// ```
/// use qubit_operation::StopReason;
///
/// fn stop_source(reason: StopReason) -> &'static str {
///     match reason {
///         StopReason::UserCancelled => "caller",
///         StopReason::Timeout => "deadline",
///         StopReason::ParentStopped => "parent operation",
///         StopReason::ManagerShutdown => "manager shutdown",
///         StopReason::RequiredChildFailed => "required child failure",
///     }
/// }
///
/// assert_eq!(stop_source(StopReason::Timeout), "deadline");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StopReason {
    /// A caller requested cancellation.
    UserCancelled,
    /// The operation's deadline expired.
    Timeout,
    /// An ancestor operation stopped.
    ParentStopped,
    /// The manager was shut down.
    ManagerShutdown,
    /// A required child failed.
    RequiredChildFailed,
}

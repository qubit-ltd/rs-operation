// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::fmt;
use std::error::Error;

/// Failure to accept work before its body has been started.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::AdmissionError;
///
/// let error = AdmissionError::Busy;
/// assert_eq!(error.to_string(), "operation capacity is busy");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum AdmissionError {
    /// All root, queue, or waiter capacity is reserved.
    Busy,
    /// New work is no longer accepted.
    Closed,
    /// The operation context has received a stop request.
    Stopped,
    /// The operation deadline elapsed before admission.
    DeadlineExpired,
    /// A finite child/resource registration budget is exhausted.
    ReservationLimit,
    /// No driver can accept the work.
    DriverUnavailable,
    /// A blocking admission wait from a managed worker would deadlock.
    WouldDeadlock,
}

impl fmt::Display for AdmissionError {
    /// Writes the human-readable description of this admission failure.
    ///
    /// # Parameters
    ///
    /// * `f` is the formatter that receives the description.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when the description is written, or the formatting
    /// error returned by the formatter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Busy => "operation capacity is busy",
            Self::Closed => "operation supervisor is closed",
            Self::Stopped => "operation context has been stopped",
            Self::DeadlineExpired => "operation deadline has expired",
            Self::ReservationLimit => "operation reservation limit reached",
            Self::DriverUnavailable => "operation driver is unavailable",
            Self::WouldDeadlock => "blocking admission wait would deadlock",
        })
    }
}

impl Error for AdmissionError {}

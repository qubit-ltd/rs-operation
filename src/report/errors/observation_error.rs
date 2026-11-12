// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use core::fmt;
use std::error::Error;

/// Failure while observing a result or settlement.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::ObservationError;
///
/// let error = ObservationError::AlreadyTaken;
/// assert_eq!(error.to_string(), "operation result was already taken");
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[must_use]
pub enum ObservationError {
    /// The one-shot typed result was already consumed.
    AlreadyTaken,
    /// The service discarded the typed result while retaining settlement
    /// responsibility.
    ResultDiscarded,
    /// A blocking observation from a framework-managed worker would deadlock.
    WouldDeadlock,
    /// The observer does not refer to a registered operation.
    InvalidObserver,
    /// The observer deadline has no configured clock source.
    ClockUnavailable,
    /// The observer deadline belongs to a different clock domain.
    ClockDomainMismatch,
    /// The operation already has the maximum number of asynchronous result
    /// waiters.
    WaiterCapacityExceeded,
}

impl fmt::Display for ObservationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AlreadyTaken => "operation result was already taken",
            Self::ResultDiscarded => "operation result was discarded",
            Self::WouldDeadlock => "blocking observation would deadlock",
            Self::InvalidObserver => "operation observer is invalid",
            Self::ClockUnavailable => "operation observer has no clock source",
            Self::ClockDomainMismatch => "operation observer deadline uses another clock domain",
            Self::WaiterCapacityExceeded => "operation has reached its asynchronous waiter capacity",
        })
    }
}

impl Error for ObservationError {}

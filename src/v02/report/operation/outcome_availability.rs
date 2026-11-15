// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Availability of an operation's result for consumers.
///
/// # Examples
///
/// Report whether a result can still be collected:
///
/// ```
/// use qubit_operation::OutcomeAvailability;
///
/// fn can_collect(availability: OutcomeAvailability) -> bool {
///     match availability {
///         OutcomeAvailability::Pending => false,
///         OutcomeAvailability::Available => true,
///         OutcomeAvailability::Taken | OutcomeAvailability::Discarded => false,
///     }
/// }
///
/// assert!(!can_collect(OutcomeAvailability::Pending));
/// assert!(can_collect(OutcomeAvailability::Available));
/// assert!(!can_collect(OutcomeAvailability::Taken));
/// assert!(!can_collect(OutcomeAvailability::Discarded));
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeAvailability {
    /// Execution has not produced a result yet.
    Pending,
    /// A result is ready to be consumed.
    Available,
    /// The result has already been taken by a consumer.
    Taken,
    /// The result was explicitly discarded.
    Discarded,
}

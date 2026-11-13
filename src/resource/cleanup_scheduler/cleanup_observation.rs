// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::CleanupPanic;
use crate::report::CleanupOutcome;
use crate::report::ResourceId;

/// Result of observing or driving a registered cleanup task.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::CleanupObservation;
///
/// let observation = CleanupObservation::Pending;
/// assert!(matches!(observation, CleanupObservation::Pending));
/// ```
#[derive(Debug)]
pub enum CleanupObservation {
    /// The cleanup future remains active and can be driven again.
    Pending,
    /// The task has already completed; this variant identifies its resource.
    AlreadySettled {
        /// Resource associated with the completed task.
        resource_id: ResourceId,
    },
    /// Its deadline was reached; the future and permit remain retained.
    Unconfirmed {
        /// Resource whose cleanup has not yet been confirmed.
        resource_id: ResourceId,
    },
    /// The supplied time and deadline belong to different clock domains.
    ClockDomainMismatch {
        /// Resource whose deadline could not be compared.
        resource_id: ResourceId,
    },
    /// The cleanup future completed or panicked and released its permit.
    Settled(
        /// The cleanup outcome, or a panic fact if polling or dropping
        /// panicked.
        Result<CleanupOutcome, CleanupPanic>,
    ),
}

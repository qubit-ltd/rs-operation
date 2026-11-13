// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Describes why a recovery owner could not be retained.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::RecoveryError;
///
/// let error = RecoveryError::CapacityExceeded;
/// assert_eq!(error, RecoveryError::CapacityExceeded);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryError {
    /// The bounded recovery table has no available owner slot.
    CapacityExceeded,
    /// A lock protecting the recovery table was poisoned.
    RegistryPoisoned,
}

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Classification of a resource registration failure.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::RegisterErrorKind;
///
/// let kind = RegisterErrorKind::CapacityExceeded;
/// assert_eq!(kind, RegisterErrorKind::CapacityExceeded);
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegisterErrorKind {
    /// No reusable resource slot is available.
    CapacityExceeded,
    /// Registry state was poisoned by a prior panic.
    RegistryPoisoned,
}

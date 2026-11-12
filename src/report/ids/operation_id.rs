// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Stable identifier for an operation within its supervisor.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::OperationId;
///
/// let operation = OperationId::new(7);
/// assert_eq!(operation.get(), 7);
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(pub u128);

impl OperationId {
    /// Construct an ID assigned by an owning supervisor or test fixture.
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    /// Return the numeric representation without changing ownership.
    pub const fn get(self) -> u128 {
        self.0
    }
}

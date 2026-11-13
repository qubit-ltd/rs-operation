// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::OperationId;

/// Resource registration identity; slot reuse must increment `generation`.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::OperationId;
/// use qubit_operation::report::ResourceId;
///
/// let operation = OperationId::new(7);
/// let resource = ResourceId::new(operation, 2, 3);
/// assert_eq!(resource.operation, operation);
/// assert_eq!(resource.slot, 2);
/// assert_eq!(resource.generation, 3);
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceId {
    /// Owning operation registration.
    pub operation: OperationId,
    /// Slot within that operation's bounded resource registry.
    pub slot: u32,
    /// Generation preventing an old ID from matching a reused slot.
    pub generation: u32,
}

impl ResourceId {
    /// Create an identifier from an owning operation and registry coordinates.
    ///
    /// # Parameters
    ///
    /// - `operation`: the operation that owns the resource registration.
    /// - `slot`: the resource slot within that operation's registry.
    /// - `generation`: the slot generation that prevents stale ID reuse.
    ///
    /// # Returns
    ///
    /// The resource identity formed from the operation and registry
    /// coordinates.
    #[inline]
    pub const fn new(operation: OperationId, slot: u32, generation: u32) -> Self {
        Self {
            operation,
            slot,
            generation,
        }
    }
}

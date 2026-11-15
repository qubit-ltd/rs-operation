// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::ManagerId;

/// Identifies one resource slot and generation within a manager.
///
/// A generation changes when a slot is reused, so stale references cannot
/// identify a later resource occupying the same slot.
///
/// # Examples
///
/// ```
/// use qubit_operation::ResourceId;
///
/// // Manager operations supply identities to callbacks and reports.
/// fn same_resource(left: ResourceId, right: ResourceId) -> bool {
///     left == right
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ResourceId {
    /// Manager that owns the identified slot.
    manager: ManagerId,
    /// Slot number within the manager's resource table.
    slot: u64,
    /// Slot generation used to reject stale identities.
    generation: u64,
}

impl ResourceId {
    /// Constructs an identity for an internally allocated resource slot.
    ///
    /// # Parameters
    ///
    /// * `manager` identifies the owning manager.
    /// * `slot` is the resource table slot number.
    /// * `generation` identifies the current use of that slot.
    #[allow(dead_code)]
    pub(crate) const fn new(manager: ManagerId, slot: u64, generation: u64) -> Self {
        Self {
            manager,
            slot,
            generation,
        }
    }

    /// Returns the manager that owns this resource identity.
    #[allow(dead_code)]
    pub(crate) const fn manager(self) -> ManagerId {
        self.manager
    }
    /// Returns the resource table slot number.
    #[allow(dead_code)]
    pub(crate) const fn slot(self) -> u64 {
        self.slot
    }
    /// Returns the slot generation captured by this identity.
    #[allow(dead_code)]
    pub(crate) const fn generation(self) -> u64 {
        self.generation
    }
}

#[cfg(test)]
mod tests {
    use super::ResourceId;
    use crate::v02::report::ManagerId;

    #[test]
    fn test_resource_identity_preserves_manager_slot_and_generation() {
        let manager = ManagerId::from_raw(41);
        let id = ResourceId::new(manager, 7, 3);
        assert_eq!(id.manager(), manager);
        assert_eq!(id.slot(), 7);
        assert_eq!(id.generation(), 3);
    }
}

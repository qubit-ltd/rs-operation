// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Opaque process-local identity for a manager-owned entity.
///
/// # Examples
///
/// Compare group identities returned by manager snapshots or reports:
///
/// ```
/// use qubit_operation::GroupId;
///
/// fn same_group(left: GroupId, right: GroupId) -> bool {
///     left == right
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct GroupId(
    /// Raw process-local identity allocated for a manager-owned group.
    u128,
);

impl GroupId {
    /// Allocates a fresh identity for internal manager construction.
    #[allow(dead_code)]
    pub(crate) fn new_unique() -> Self {
        Self(super::super::internal::id_allocator::next_id())
    }

    /// Wraps a raw identity for internal reconstruction and tests.
    ///
    /// # Parameters
    ///
    /// * `value` is the raw identity to wrap.
    #[allow(dead_code)]
    pub(crate) const fn from_raw(value: u128) -> Self {
        Self(value)
    }

    /// Exposes the raw integer only within this crate.
    #[allow(dead_code)]
    pub(crate) const fn raw(self) -> u128 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::GroupId;

    #[test]
    fn test_raw_identity_round_trips_and_allocations_are_distinct() {
        let value = 0x1234_5678_9abc_def0u128;
        assert_eq!(GroupId::from_raw(value).raw(), value);
        assert_ne!(GroupId::new_unique(), GroupId::new_unique());
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::super::GroupCore;
use super::super::lock;

/// Tracks one group operation through settlement and its optional capacity
/// slot.
///
/// Settlement releases the unsettled count; dropping the permit returns any
/// reserved capacity slot and wakes group waiters.
#[must_use = "retain the permit while the group capacity slot is reserved"]
pub(crate) struct GroupPermit {
    /// Group whose unsettled operation count is held by this permit.
    pub(super) group: Arc<GroupCore>,
    /// Whether this submission reserved capacity beyond its parent.
    pub(super) capacity_reserved: bool,
    /// Whether settlement already removed this operation from the unsettled
    /// count.
    pub(super) settlement_released: bool,
}

impl GroupPermit {
    /// Removes this operation from the unsettled count after settlement.
    pub(crate) fn settlement_finished(&mut self) {
        if !self.settlement_released {
            lock(&self.group.state).unsettled_operations -= 1;
            self.settlement_released = true;
            self.group.changed.notify_all();
            self.group.notify.notify_waiters();
        }
    }
}

impl Drop for GroupPermit {
    /// Returns any capacity slot and unsettled reservation not already
    /// released.
    ///
    /// # Parameters
    ///
    /// - `self`: Permit whose active slot is returned.
    fn drop(&mut self) {
        let mut state = lock(&self.group.state);
        if !self.settlement_released {
            state.unsettled_operations -= 1;
            self.settlement_released = true;
        }
        if self.capacity_reserved {
            state.active -= 1;
        }
        drop(state);
        self.group.changed.notify_all();
        self.group.notify.notify_waiters();
    }
}

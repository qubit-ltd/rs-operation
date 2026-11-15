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

/// Owns one reserved slot in a group's active operation capacity.
///
/// Dropping an unreleased permit returns the slot and wakes group waiters.
#[must_use = "retain the permit while the group capacity slot is reserved"]
pub(crate) struct GroupPermit {
    /// Group whose active operation count is held by this permit.
    pub(super) group: Arc<GroupCore>,
    /// Whether the capacity slot has already been returned.
    pub(super) released: bool,
}

impl Drop for GroupPermit {
    /// Returns the reserved group slot when this permit has not been released.
    ///
    /// # Parameters
    ///
    /// - `self`: Permit whose active slot is returned.
    fn drop(&mut self) {
        if !self.released {
            lock(&self.group.state).active -= 1;
            self.released = true;
            self.group.changed.notify_all();
            self.group.notify.notify_waiters();
        }
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::manager::core::ManagerCore;
use crate::v02::manager::group::GroupCore;

/// Shared target observed by a shutdown handle.
#[must_use = "retain the target while observing shutdown"]
#[derive(Clone)]
pub(in crate::v02::manager::shutdown) enum ShutdownTarget {
    /// Observes shutdown of the manager as a whole.
    Manager(
        /// Manager whose lifecycle is observed.
        Arc<ManagerCore>,
    ),
    /// Observes shutdown of one group within its owning manager.
    Group(
        /// Parent manager that owns the group.
        Arc<ManagerCore>,
        /// Group whose shutdown is observed.
        Arc<GroupCore>,
    ),
}

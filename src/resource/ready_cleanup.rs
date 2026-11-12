// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use crate::protocol::OwnedCleanup;
use crate::report::ResourceId;

/// Cleanup owner removed from the registry and ready for an external cleanup
/// driver.
pub struct ReadyCleanup {
    pub(super) resource_id: ResourceId,
    pub(super) owner: Option<Box<dyn OwnedCleanup>>,
}

impl ReadyCleanup {
    /// Return the resource identity whose cleanup owner this value carries.
    pub const fn resource_id(&self) -> ResourceId {
        self.resource_id
    }

    /// Give the owned adapter to a cleanup driver without running it here.
    pub fn into_parts(mut self) -> (ResourceId, Box<dyn OwnedCleanup>) {
        (
            self.resource_id,
            self.owner.take().expect("ready cleanup owner is present"),
        )
    }
}

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Opaque identity for a task accepted by a [`super::PortableDriver`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TaskHandle(pub(super) u64);

impl TaskHandle {
    /// Return the stable identifier used by an external driver adapter.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}

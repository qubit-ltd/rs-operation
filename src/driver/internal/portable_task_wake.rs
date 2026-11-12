// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Weak;
use std::task::Wake;

use super::PortableShared;
use super::schedule_portable_task;

/// Waker that requeues one registered task at most once.
pub struct PortableTaskWake {
    /// Weak reference to shared portable-driver state.
    pub shared: Weak<PortableShared>,
    /// Identifier of the task to schedule when this waker fires.
    pub id: u64,
}

impl Wake for PortableTaskWake {
    #[inline]
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let Some(shared) = self.shared.upgrade() else {
            return;
        };
        schedule_portable_task(&shared, self.id);
    }
}

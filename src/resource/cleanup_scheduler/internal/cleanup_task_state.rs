// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

#[cfg(test)]
use std::sync::Arc;
use std::sync::Mutex;

use super::CleanupTaskInner;

/// Synchronization wrapper shared by observers of one cleanup task.
pub(in crate::resource::cleanup_scheduler) struct CleanupTaskState {
    /// Mutable task data protected while the future is moved out for polling.
    pub(in crate::resource::cleanup_scheduler) inner: Mutex<CleanupTaskInner>,
    /// Test-only rendezvous used to control two observers competing to poll.
    #[cfg(test)]
    pub(in crate::resource::cleanup_scheduler) poll_gate: Mutex<Option<Arc<PollGate>>>,
}

#[cfg(test)]
pub(in crate::resource::cleanup_scheduler) struct PollGate {
    state: Mutex<(usize, bool)>,
    changed: std::sync::Condvar,
}

#[cfg(test)]
impl PollGate {
    pub(in crate::resource::cleanup_scheduler) fn new() -> Self {
        Self {
            state: Mutex::new((0, false)),
            changed: std::sync::Condvar::new(),
        }
    }

    pub(in crate::resource::cleanup_scheduler) fn wait(&self) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        state.0 += 1;
        self.changed.notify_all();
        while !state.1 {
            let (next, timeout) = self
                .changed
                .wait_timeout(state, std::time::Duration::from_secs(5))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            assert!(
                !timeout.timed_out() || state.1,
                "cleanup poll gate timed out waiting for release"
            );
        }
    }

    pub(in crate::resource::cleanup_scheduler) fn release_when_ready(&self) {
        let mut state = self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        while state.0 < 2 {
            let (next, timeout) = self
                .changed
                .wait_timeout(state, std::time::Duration::from_secs(5))
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            assert!(
                !timeout.timed_out() || state.0 >= 2,
                "cleanup poll gate timed out waiting for observers"
            );
        }
        state.1 = true;
        self.changed.notify_all();
    }
}

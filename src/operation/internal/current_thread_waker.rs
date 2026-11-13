// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Thread waker used by blocking admission waits.

use std::sync::Arc;

/// Waker that unparks the thread currently blocking on admission.
pub(crate) struct CurrentThreadWaker(
    /// Thread to unpark when the admission future becomes ready.
    pub(crate) std::thread::Thread,
);

impl std::task::Wake for CurrentThreadWaker {
    /// Unparks the waiting thread.
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    /// Unparks the waiting thread without consuming the shared waker.
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

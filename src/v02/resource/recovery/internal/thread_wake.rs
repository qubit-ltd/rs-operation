// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::task::Wake;
use std::thread::Thread;

/// Waker implementation that unparks the thread driving a future.
///
/// `block_on` uses this value to sleep between polls and resume when the future
/// signals that it may make progress.
pub(super) struct ThreadWake(
    /// Thread to unpark when a future schedules another poll.
    pub(super) Thread,
);

impl Wake for ThreadWake {
    /// Unparks the thread after consuming this shared waker.
    ///
    /// # Parameters
    ///
    /// * `self` — waker that owns the thread handle.
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    /// Unparks the thread while retaining this shared waker.
    ///
    /// # Parameters
    ///
    /// * `self` — borrowed waker that owns the thread handle.
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Wake;

    use super::ThreadWake;

    #[test]
    fn test_wake_callbacks_unpark_the_owned_thread() {
        let wake = Arc::new(ThreadWake(std::thread::current()));

        Wake::wake_by_ref(&wake);
        std::thread::park();
        Wake::wake(Arc::clone(&wake));
        std::thread::park();
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

/// Wake implementation that unparks the synchronous executor thread.
pub(in crate::v02::manager::core) struct ThreadUnparker(
    /// Thread that is unparked when its executor waker is invoked.
    pub(in crate::v02::manager::core) std::thread::Thread,
);
impl std::task::Wake for ThreadUnparker {
    /// Unparks the owning thread when an executor wake consumes its waker.
    #[inline]
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    /// Unparks the owning thread while retaining the shared waker.
    #[inline]
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Wake;

    use super::ThreadUnparker;

    #[test]
    fn test_wake_callbacks_unpark_the_executor_thread() {
        let unparker = Arc::new(ThreadUnparker(std::thread::current()));

        Wake::wake_by_ref(&unparker);
        std::thread::park();
        Wake::wake(Arc::clone(&unparker));
        std::thread::park();
    }
}

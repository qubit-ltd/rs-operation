// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use super::Inner;
use super::Notified;
use super::lock;
use crate::v02::manager::core::dispose_panic_payload;

/// A latched notification source for blocking and asynchronous observers.
pub(crate) struct Notify {
    /// Shared generation, waiter list, and blocking observers for this source.
    pub(in crate::v02::internal::notify) inner: Arc<Inner>,
}

impl Notify {
    /// Creates a notification source with generation zero.
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }

    /// Returns the current generation.
    pub(crate) fn generation(&self) -> u64 {
        lock(&self.inner.state).generation
    }

    /// Returns the active waiter count for internal regression tests.
    #[cfg(test)]
    pub(crate) fn waiter_count(&self) -> usize {
        lock(&self.inner.state).waiters.len()
    }

    /// Creates a future observing changes after the current generation.
    pub(crate) fn notified(&self) -> Notified {
        self.notified_since(self.generation())
    }

    /// Creates a future observing changes after `generation`.
    ///
    /// # Parameters
    ///
    /// - `self`: Notification source whose changes are observed.
    /// - `generation`: Last observed generation; a later generation wakes the
    ///   returned future.
    pub(crate) fn notified_since(&self, generation: u64) -> Notified {
        Notified {
            inner: Arc::downgrade(&self.inner),
            generation,
            waiter_id: None,
        }
    }

    /// Advances the generation and wakes current observers outside the lock.
    pub(crate) fn notify_waiters(&self) {
        let wakers = {
            let mut state = lock(&self.inner.state);
            state.generation = state.generation.wrapping_add(1);
            std::mem::take(&mut state.waiters)
                .into_iter()
                .map(|(_, waker)| waker)
                .collect::<Vec<_>>()
        };
        self.inner.changed.notify_all();
        for waker in wakers {
            // A task waker is user supplied code. One misbehaving observer
            // must not prevent the remaining waiters from being notified.
            if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| waker.wake())) {
                dispose_panic_payload(payload);
            }
        }
    }

    /// Blocks until the generation differs from `generation`.
    ///
    /// # Parameters
    ///
    /// - `self`: Notification source whose changes are awaited.
    /// - `generation`: Last observed generation.
    pub(crate) fn wait_blocking_since(&self, generation: u64) {
        let mut state = lock(&self.inner.state);
        while state.generation == generation {
            state = self
                .inner
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::panic;
    use std::panic::AssertUnwindSafe;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    use super::super::Notified;
    use super::Notify;

    struct PanicOnDrop;

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("panic payload drop");
        }
    }

    struct PanickingWake;

    impl Wake for PanickingWake {
        fn wake(self: Arc<Self>) {
            panic::panic_any(PanicOnDrop);
        }
    }

    struct CountWake(Arc<AtomicUsize>);

    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn register_waiter(notify: &Notify, waker: &Waker) -> Pin<Box<Notified>> {
        let mut notified = Box::pin(notify.notified());
        let mut context = Context::from_waker(waker);
        assert!(matches!(notified.as_mut().poll(&mut context), Poll::Pending));
        notified
    }

    #[test]
    fn test_panic_payload_drop_does_not_skip_later_waiters() {
        let notify = Notify::new();
        let later_wakes = Arc::new(AtomicUsize::new(0));
        let first = Waker::from(Arc::new(PanickingWake));
        let later = Waker::from(Arc::new(CountWake(Arc::clone(&later_wakes))));
        let _first_waiter = register_waiter(&notify, &first);
        let _later_waiter = register_waiter(&notify, &later);

        let result = panic::catch_unwind(AssertUnwindSafe(|| notify.notify_waiters()));

        assert!(result.is_ok(), "panic payload disposal escaped notification");
        assert_eq!(later_wakes.load(Ordering::SeqCst), 1);
    }
}

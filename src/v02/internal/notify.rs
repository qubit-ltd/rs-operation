// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Latched notifications shared by blocking and asynchronous observers.

mod internal;
mod notified;
#[path = "notify/notify.rs"]
mod signal;

pub(in crate::v02::internal::notify) use internal::Inner;
pub(in crate::v02::internal::notify) use internal::lock;
pub(crate) use notified::Notified;
pub(crate) use signal::Notify;

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::Weak;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Waker;

    use super::Inner;
    use super::Notified;
    use super::Notify;
    use super::lock;

    /// Two-phase gate for controlling a publisher without timing assumptions.
    #[derive(Default)]
    struct Gate {
        state: Mutex<(bool, bool)>,
        changed: Condvar,
    }

    impl Gate {
        fn enter_and_wait(&self) {
            let mut state = lock(&self.state);
            state.0 = true;
            self.changed.notify_all();
            while !state.1 {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }

        fn wait_started(&self) {
            let mut state = lock(&self.state);
            while !state.0 {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }

        fn release(&self) {
            lock(&self.state).1 = true;
            self.changed.notify_all();
        }
    }

    fn poll_once(future: Pin<&mut Notified>, waker: &Waker) -> Poll<()> {
        let mut context = Context::from_waker(waker);
        future.poll(&mut context)
    }

    #[test]
    fn test_registration_racing_with_publish_observes_notification() {
        let notify = Arc::new(Notify::new());
        let gate = Arc::new(Gate::default());
        let generation = notify.generation();
        let publisher = {
            let notify = Arc::clone(&notify);
            let gate = Arc::clone(&gate);
            std::thread::spawn(move || {
                gate.enter_and_wait();
                notify.notify_waiters();
            })
        };
        gate.wait_started();
        let mut notified = Box::pin(notify.notified_since(generation));
        let waker = Waker::noop();
        assert!(poll_once(notified.as_mut(), waker).is_pending());
        gate.release();
        publisher.join().expect("publisher thread should finish");
        assert!(poll_once(notified.as_mut(), waker).is_ready());

        // Also cover publication before first registration; the observed
        // generation makes the future complete without requiring a wake.
        let old_generation = notify.generation();
        notify.notify_waiters();
        let mut late = Box::pin(notify.notified_since(old_generation));
        assert!(poll_once(late.as_mut(), waker).is_ready());
    }

    #[test]
    fn test_cancelled_wait_removes_registration() {
        let notify = Notify::new();
        let mut notified = Box::pin(notify.notified());
        let waker = Waker::noop();
        assert!(poll_once(notified.as_mut(), waker).is_pending());
        assert_eq!(lock(&notify.inner.state).waiters.len(), 1);
        drop(notified);
        assert!(lock(&notify.inner.state).waiters.is_empty());
    }

    struct ReentrantWaker {
        inner: Weak<Inner>,
        outside_lock: AtomicBool,
    }

    impl ReentrantWaker {
        fn check(&self) {
            let inner = self.inner.upgrade().expect("notification should remain alive");
            let readable = match inner.state.try_lock() {
                Ok(state) => {
                    let _observed_generation = state.generation;
                    true
                }
                Err(_) => false,
            };
            self.outside_lock.store(readable, Ordering::SeqCst);
        }
    }

    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        // SAFETY: the RawWaker vtable receives the live Arc pointer installed below.
        let arc = std::mem::ManuallyDrop::new(unsafe { Arc::<ReentrantWaker>::from_raw(data.cast()) });
        arc.check();
        let cloned = Arc::clone(&arc);
        RawWaker::new(Arc::into_raw(cloned).cast(), &VTABLE)
    }

    unsafe fn wake_waker(data: *const ()) {
        // SAFETY: wake consumes one pointer previously produced by Arc::into_raw.
        let arc = unsafe { Arc::<ReentrantWaker>::from_raw(data.cast()) };
        arc.check();
    }

    unsafe fn wake_by_ref_waker(data: *const ()) {
        // SAFETY: by-ref wake borrows the live pointer installed in the RawWaker.
        let arc = std::mem::ManuallyDrop::new(unsafe { Arc::<ReentrantWaker>::from_raw(data.cast()) });
        arc.check();
    }

    unsafe fn drop_waker(data: *const ()) {
        // SAFETY: drop consumes one pointer previously produced by Arc::into_raw.
        let arc = unsafe { Arc::<ReentrantWaker>::from_raw(data.cast()) };
        arc.check();
    }

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref_waker, drop_waker);

    #[test]
    fn test_reentrant_waker_runs_outside_lock() {
        let notify = Notify::new();
        let probe = Arc::new(ReentrantWaker {
            inner: Arc::downgrade(&notify.inner),
            outside_lock: AtomicBool::new(true),
        });
        let raw = RawWaker::new(Arc::into_raw(Arc::clone(&probe)).cast(), &VTABLE);
        let waker = unsafe { Waker::from_raw(raw) };
        let mut notified = Box::pin(notify.notified());
        assert!(poll_once(notified.as_mut(), &waker).is_pending());
        assert!(
            probe.outside_lock.load(Ordering::SeqCst),
            "waker clone must run outside lock"
        );
        probe.outside_lock.store(true, Ordering::SeqCst);
        notify.notify_waiters();
        assert!(probe.outside_lock.load(Ordering::SeqCst), "wake must run outside lock");
        drop(notified);

        let mut cancelled = Box::pin(notify.notified());
        assert!(poll_once(cancelled.as_mut(), &waker).is_pending());
        assert!(
            probe.outside_lock.load(Ordering::SeqCst),
            "replacement waker clone must run outside lock"
        );
        probe.outside_lock.store(true, Ordering::SeqCst);
        drop(cancelled);
        assert!(
            probe.outside_lock.load(Ordering::SeqCst),
            "waker destruction must run outside lock"
        );
        drop(waker);
        assert!(
            probe.outside_lock.load(Ordering::SeqCst),
            "context waker destruction must run outside lock"
        );
    }
}

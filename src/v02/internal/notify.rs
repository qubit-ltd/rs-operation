//! Latched notifications shared by blocking and asynchronous observers.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::Weak;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

/// A latched notification source for blocking and asynchronous observers.
pub(crate) struct Notify {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    state: Mutex<WaitState>,
    changed: Condvar,
    next_waiter: AtomicU64,
}

#[derive(Default)]
struct WaitState {
    generation: u64,
    waiters: Vec<(u64, Waker)>,
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

    /// Creates a future observing changes after the current generation.
    pub(crate) fn notified(&self) -> Notified {
        self.notified_since(self.generation())
    }

    /// Creates a future observing changes after `generation`.
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
            waker.wake();
        }
    }

    /// Blocks until the generation differs from `generation`.
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

/// Future that completes after the notification generation changes.
pub(crate) struct Notified {
    inner: Weak<Inner>,
    generation: u64,
    waiter_id: Option<u64>,
}

impl Future for Notified {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(inner) = self.inner.upgrade() else {
            self.waiter_id = None;
            return Poll::Ready(());
        };
        let id = *self
            .waiter_id
            .get_or_insert_with(|| inner.next_waiter.fetch_add(1, Ordering::Relaxed));
        // Waker clone can invoke a user-defined RawWaker callback, so it must
        // not run while the waiter-list lock is held.
        let mut new_waker = Some(context.waker().clone());
        let mut removed_waker = None;
        let changed = {
            let mut state = lock(&inner.state);
            if state.generation != self.generation {
                if let Some(index) = state
                    .waiters
                    .iter()
                    .position(|(waiter_id, _)| *waiter_id == id)
                {
                    removed_waker = Some(state.waiters.swap_remove(index).1);
                }
                true
            } else if let Some((_, existing)) = state
                .waiters
                .iter_mut()
                .find(|(waiter_id, _)| *waiter_id == id)
            {
                if !existing.will_wake(new_waker.as_ref().expect("new waker is present")) {
                    removed_waker = Some(std::mem::replace(
                        existing,
                        new_waker.take().expect("new waker is present"),
                    ));
                }
                false
            } else {
                state
                    .waiters
                    .push((id, new_waker.take().expect("new waker is present")));
                false
            }
        };
        // Waker drop can also run user-defined code.
        drop(removed_waker);
        drop(new_waker);
        if changed {
            self.waiter_id = None;
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for Notified {
    fn drop(&mut self) {
        let Some(id) = self.waiter_id.take() else {
            return;
        };
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let removed = {
            let mut state = lock(&inner.state);
            state
                .waiters
                .iter()
                .position(|(waiter_id, _)| *waiter_id == id)
                .map(|index| state.waiters.swap_remove(index).1)
        };
        drop(removed);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{RawWaker, RawWakerVTable, Waker};

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
    fn register_races_publish() {
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
    fn cancelled_wait_removes_registration() {
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
            let inner = self
                .inner
                .upgrade()
                .expect("notification should remain alive");
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
        let arc =
            std::mem::ManuallyDrop::new(unsafe { Arc::<ReentrantWaker>::from_raw(data.cast()) });
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
        let arc =
            std::mem::ManuallyDrop::new(unsafe { Arc::<ReentrantWaker>::from_raw(data.cast()) });
        arc.check();
    }

    unsafe fn drop_waker(data: *const ()) {
        // SAFETY: drop consumes one pointer previously produced by Arc::into_raw.
        let arc = unsafe { Arc::<ReentrantWaker>::from_raw(data.cast()) };
        arc.check();
    }

    static VTABLE: RawWakerVTable =
        RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref_waker, drop_waker);

    #[test]
    fn reentrant_waker_runs_outside_lock() {
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
        assert!(
            probe.outside_lock.load(Ordering::SeqCst),
            "wake must run outside lock"
        );
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

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

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

use crate::report::ObservationError;

/// Maximum number of asynchronous waiters retained by one notification source.
///
/// Additional registrations fail with
/// [`ObservationError::WaiterCapacityExceeded`].
pub(crate) const ASYNC_WAITER_CAPACITY: usize = 64;

/// Mutable waiter list guarded with the notification generation.
#[derive(Default)]
struct WaitState {
    /// Incremented whenever state becomes observable to waiters.
    generation: u64,
    /// Async waiters registered for the current generation.
    waiters: Vec<(u64, Waker)>,
}

/// Shared synchronization backing one notification source.
#[derive(Default)]
struct Inner {
    /// Serializes generation and waiter registration.
    state: Mutex<WaitState>,
    /// Wakes blocking observers after a generation change.
    changed: Condvar,
    /// Allocates distinct waiter identities for independent futures.
    next_waiter: AtomicU64,
}

/// A latched notification source for blocking and asynchronous observers.
pub(crate) struct Notify {
    /// Shared backing keeps in-flight `Notified` futures valid.
    inner: Arc<Inner>,
}

impl Notify {
    /// Create a notification source with generation zero.
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Inner::default()),
        }
    }

    /// Return the current generation for read-register-recheck wait protocols.
    pub(crate) fn generation(&self) -> u64 {
        lock(&self.inner.state).generation
    }

    /// Create a future observing changes after the current generation.
    pub(crate) fn notified(&self) -> Notified {
        self.notified_since(self.generation())
    }

    /// Create a future observing changes after a previously read generation.
    pub(crate) fn notified_since(&self, generation: u64) -> Notified {
        Notified {
            inner: Arc::downgrade(&self.inner),
            generation,
            waiter_id: None,
        }
    }

    /// Advance the generation and wake all current observers outside the lock.
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

    /// Block until a generation change, checking the predicate around Condvar
    /// wait.
    pub(crate) fn wait_blocking_since(&self, generation: u64) {
        let mut state = lock(&self.inner.state);
        while state.generation == generation {
            state = match self.inner.changed.wait(state) {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }
}

/// Future that completes after the notification generation changes.
pub(crate) struct Notified {
    /// Weak ownership avoids keeping its state cell alive.
    inner: Weak<Inner>,
    /// Generation this future waits to advance beyond.
    generation: u64,
    /// Unique registration removed when this future is dropped or completes.
    waiter_id: Option<u64>,
}

impl Future for Notified {
    type Output = Result<(), ObservationError>;

    /// Register this future or complete when its notification generation
    /// advances.
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(inner) = self.inner.upgrade() else {
            self.waiter_id = None;
            return Poll::Ready(Ok(()));
        };
        let id = *self
            .waiter_id
            .get_or_insert_with(|| inner.next_waiter.fetch_add(1, Ordering::Relaxed));
        let mut replaced_waker = None;
        let registration = {
            let mut state = lock(&inner.state);
            if state.generation != self.generation {
                if let Some(index) = state.waiters.iter().position(|(waiter_id, _)| *waiter_id == id) {
                    replaced_waker = Some(state.waiters.swap_remove(index).1);
                }
                Ok(true)
            } else if let Some((_, existing)) = state.waiters.iter_mut().find(|(waiter_id, _)| *waiter_id == id) {
                if !existing.will_wake(context.waker()) {
                    replaced_waker = Some(std::mem::replace(existing, context.waker().clone()));
                }
                Ok(false)
            } else {
                if state.waiters.len() >= ASYNC_WAITER_CAPACITY {
                    Err(ObservationError::WaiterCapacityExceeded)
                } else {
                    state.waiters.push((id, context.waker().clone()));
                    Ok(false)
                }
            }
        };
        drop(replaced_waker);
        match registration {
            Ok(true) => {
                self.waiter_id = None;
                Poll::Ready(Ok(()))
            }
            Ok(false) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
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

/// Recover a poisoned internal synchronization lock without exposing poison.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;

    use super::ASYNC_WAITER_CAPACITY;
    use super::Notify;
    use crate::report::ObservationError;

    struct CountingWake(Arc<Mutex<usize>>);

    impl Wake for CountingWake {
        fn wake(self: Arc<Self>) {
            *self.0.lock().unwrap() += 1;
        }

        fn wake_by_ref(self: &Arc<Self>) {
            *self.0.lock().unwrap() += 1;
        }
    }

    fn counting_waker() -> (Waker, Arc<Mutex<usize>>) {
        let count = Arc::new(Mutex::new(0));
        (Waker::from(Arc::new(CountingWake(Arc::clone(&count)))), count)
    }

    #[test]
    fn test_notification_before_wait_is_latched() {
        let notify = Notify::new();
        let observed = notify.generation();
        notify.notify_waiters();
        let mut notified = Box::pin(notify.notified_since(observed));
        let (waker, _) = counting_waker();
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            Pin::as_mut(&mut notified).poll(&mut context),
            Poll::Ready(Ok(()))
        ));
    }

    #[test]
    fn test_notification_after_wait_registration_wakes_once() {
        let notify = Notify::new();
        let mut notified = Box::pin(notify.notified());
        let (waker, wake_count) = counting_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(Pin::as_mut(&mut notified).poll(&mut context), Poll::Pending));

        notify.notify_waiters();

        assert_eq!(*wake_count.lock().unwrap(), 1);
        assert!(matches!(
            Pin::as_mut(&mut notified).poll(&mut context),
            Poll::Ready(Ok(()))
        ));
    }

    #[test]
    fn test_observers_keep_independent_wakers_and_repeated_poll_is_stable() {
        let notify = Notify::new();
        let mut first = Box::pin(notify.notified());
        let mut second = Box::pin(notify.notified());
        let (first_waker, first_count) = counting_waker();
        let (second_waker, second_count) = counting_waker();
        let mut first_context = Context::from_waker(&first_waker);
        let mut second_context = Context::from_waker(&second_waker);

        assert!(matches!(
            Pin::as_mut(&mut first).poll(&mut first_context),
            Poll::Pending
        ));
        assert!(matches!(
            Pin::as_mut(&mut first).poll(&mut first_context),
            Poll::Pending
        ));
        assert!(matches!(
            Pin::as_mut(&mut second).poll(&mut second_context),
            Poll::Pending
        ));

        notify.notify_waiters();

        assert_eq!(*first_count.lock().unwrap(), 1);
        assert_eq!(*second_count.lock().unwrap(), 1);
    }

    #[test]
    fn test_dropping_one_waiter_does_not_remove_another_waiter() {
        let notify = Notify::new();
        let mut first = Box::pin(notify.notified());
        let mut second = Box::pin(notify.notified());
        let (first_waker, first_count) = counting_waker();
        let (second_waker, second_count) = counting_waker();
        let mut first_context = Context::from_waker(&first_waker);
        let mut second_context = Context::from_waker(&second_waker);
        assert!(matches!(
            Pin::as_mut(&mut first).poll(&mut first_context),
            Poll::Pending
        ));
        assert!(matches!(
            Pin::as_mut(&mut second).poll(&mut second_context),
            Poll::Pending
        ));
        drop(first);

        notify.notify_waiters();

        assert_eq!(*first_count.lock().unwrap(), 0);
        assert_eq!(*second_count.lock().unwrap(), 1);
    }

    #[test]
    fn test_async_waiter_capacity_is_enforced_and_reclaimed_on_drop() {
        let notify = Notify::new();
        let (waker, _) = counting_waker();
        let mut context = Context::from_waker(&waker);
        let mut waiters = (0..ASYNC_WAITER_CAPACITY)
            .map(|_| Box::pin(notify.notified()))
            .collect::<Vec<_>>();

        for waiter in &mut waiters {
            assert!(matches!(waiter.as_mut().poll(&mut context), Poll::Pending));
        }

        let mut overflow = Box::pin(notify.notified());
        assert!(matches!(
            overflow.as_mut().poll(&mut context),
            Poll::Ready(Err(ObservationError::WaiterCapacityExceeded))
        ));
        drop(overflow);

        drop(waiters.pop());
        let mut replacement = Box::pin(notify.notified());
        assert!(matches!(replacement.as_mut().poll(&mut context), Poll::Pending));
    }
}

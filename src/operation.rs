// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Public managed operation handles and supervisor submission.

use core::future::Future;
use core::task::Context;
use core::task::Poll;
use core::task::Waker;
use std::sync::Arc;
use std::sync::Mutex;

use self::internal::CurrentThreadWaker;
use self::internal::Shared;
use self::internal::State;
pub use self::observe_until::ObserveUntil;
pub use self::observer_snapshot::ObserverSnapshot;
pub use self::operation_handle::OperationHandle;
pub use self::operation_observer::OperationObserver;
use crate::report::ObservationError;
use crate::report::OperationOutcome;
use crate::report::ResourcePhase;
use crate::report::SettlementReport;

pub(crate) mod internal;
mod observe_until;
mod observer_snapshot;
mod operation_handle;
mod operation_observer;

/// Builds a non-consuming view of result and settlement state.
///
/// # Type Parameters
///
/// `T` and `E` identify the operation result and application error represented
/// by `state`.
///
/// # Parameters
///
/// * `state` is the shared result state to inspect without consuming it.
/// * `settlement` is `Some(report)` after cleanup completes and `None` while
///   settlement is still pending.
///
/// # Returns
///
/// A snapshot combining the current result state and optional settlement.
fn observer_snapshot<T, E>(state: &State<T, E>, settlement: Option<SettlementReport>) -> ObserverSnapshot {
    ObserverSnapshot {
        resource_phase: settlement
            .as_ref()
            .map_or(ResourcePhase::Active, |report| report.resource_phase),
        business_result_ready: state.result.is_some() || state.result_taken || state.result_discarded,
        result_taken: state.result_taken,
        result_discarded: state.result_discarded,
        settlement,
    }
}

/// Polls for the unique result and registers a task waker while it is pending.
///
/// # Type Parameters
///
/// `T` and `E` are the operation's result and application error types.
///
/// # Parameters
///
/// * `shared` contains the result state and registered waiters.
/// * `cx` provides the task waker to register while the result is pending.
///
/// # Returns
///
/// `Pending` while waiting, or the unique outcome/error when it is ready.
fn poll_result<T, E>(
    shared: &Arc<Shared<T, E>>,
    cx: &mut Context<'_>,
) -> Poll<Result<OperationOutcome<T, E>, ObservationError>> {
    let mut candidate = Some(cx.waker().clone());
    let (result, lease, discarded, already) = {
        let mut state = lock(&shared.state);
        if state.result_taken {
            (None, None, false, true)
        } else if state.result_discarded {
            (None, None, true, false)
        } else if let Some(result) = state.result.take() {
            state.result_taken = true;
            (Some(result), state.result_lease.take(), false, false)
        } else {
            if !state.result_waiters.iter().any(|w| w.will_wake(cx.waker())) {
                state
                    .result_waiters
                    .push(candidate.take().expect("candidate waker is present"));
            }
            (None, None, false, false)
        }
    };
    drop(candidate);
    if already {
        return Poll::Ready(Err(ObservationError::AlreadyTaken));
    }
    if discarded {
        return Poll::Ready(Err(ObservationError::ResultDiscarded));
    }
    if let Some(result) = result {
        if let Some(lease) = lease {
            lease.observe();
        }
        Poll::Ready(Ok(result))
    } else {
        Poll::Pending
    }
}
/// Polls for settlement and registers a task waker while it is pending.
///
/// # Type Parameters
///
/// `T` and `E` are the operation's result and application error types.
///
/// # Parameters
///
/// * `shared` contains the settlement state and registered waiters.
/// * `cx` provides the task waker to register while settlement is pending.
///
/// # Returns
///
/// `Pending` while cleanup continues, or the published settlement report.
fn poll_released<T, E>(shared: &Arc<Shared<T, E>>, cx: &mut Context<'_>) -> Poll<SettlementReport> {
    let mut candidate = Some(cx.waker().clone());
    let result = {
        let mut state = lock(&shared.state);
        if let Some(report) = &state.released {
            Poll::Ready(report.clone())
        } else {
            if !state.release_waiters.iter().any(|w| w.will_wake(cx.waker())) {
                state
                    .release_waiters
                    .push(candidate.take().expect("candidate waker is present"));
            }
            Poll::Pending
        }
    };
    drop(candidate);
    result
}

/// Removes a timed-out observer's waker and drops it after releasing the lock.
///
/// # Type Parameters
///
/// `T` and `E` describe the shared operation state.
///
/// # Parameters
///
/// * `shared` contains the registered release waiters.
/// * `waker` identifies the timed-out observer to remove.
fn remove_release_waiter<T, E>(shared: &Arc<Shared<T, E>>, waker: &Waker) {
    let removed = {
        let mut state = lock(&shared.state);
        let mut retained = Vec::with_capacity(state.release_waiters.len());
        let mut removed = Vec::new();
        for waiter in state.release_waiters.drain(..) {
            if waiter.will_wake(waker) {
                removed.push(waiter);
            } else {
                retained.push(waiter);
            }
        }
        state.release_waiters = retained;
        removed
    };
    drop(removed);
}

/// Publishes the outcome and wakes result waiters after releasing the state
/// lock.
///
/// # Type Parameters
///
/// `T` and `E` are the outcome's result and application error types.
///
/// # Parameters
///
/// * `shared` is the state where the outcome is published.
/// * `outcome` is the completed operation outcome to publish.
pub(crate) fn publish_result<T, E>(shared: &Shared<T, E>, outcome: OperationOutcome<T, E>) {
    let (waiters, discarded) = {
        let mut state = lock(&shared.state);
        if state.result_discarded {
            (Vec::new(), Some(outcome))
        } else {
            state.result = Some(outcome);
            (std::mem::take(&mut state.result_waiters), None)
        }
    };
    drop(discarded);
    shared.changed.notify_all();
    for waiter in waiters {
        waiter.wake();
    }
}
/// Publishes final settlement and releases completion guards outside the lock.
///
/// # Type Parameters
///
/// `T` and `E` describe the operation whose settlement is being published.
///
/// # Parameters
///
/// * `shared` is the state where settlement is published.
/// * `report` describes the completed cleanup and resource settlement.
pub(crate) fn publish_released<T, E>(shared: &Shared<T, E>, report: SettlementReport) {
    let (waiters, guards) = {
        let mut state = lock(&shared.state);
        state.released = Some(report);
        (
            std::mem::take(&mut state.release_waiters),
            std::mem::take(&mut state.completion_guards),
        )
    };
    drop(guards);
    shared.changed.notify_all();
    for waiter in waiters {
        waiter.wake();
    }
}
/// Recovers a poisoned mutex so operation cleanup can continue.
///
/// # Type Parameters
///
/// `T` is the data protected by the mutex.
///
/// # Parameters
///
/// `mutex` is the operation state lock to acquire.
///
/// # Returns
///
/// The mutex guard, recovering its inner value if it was poisoned.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// Drives admission by parking until its waker unparks this thread.
///
/// # Type Parameters
///
/// `F` is the future that performs admission.
///
/// # Parameters
///
/// `future` is polled until it returns `Ready`.
///
/// # Returns
///
/// The output produced by the admission future.
pub(crate) fn block_on_start_wait<F: Future>(future: F) -> F::Output {
    let waker = std::task::Waker::from(Arc::new(CurrentThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(result) = future.as_mut().poll(&mut context) {
            return result;
        }
        std::thread::park();
    }
}

#[cfg(test)]
mod tests {
    use core::task::Context;
    use core::task::RawWaker;
    use core::task::RawWakerVTable;
    use core::task::Waker;
    use std::sync::Arc;
    use std::sync::Condvar;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Poll;

    use super::Shared;
    use super::State;
    use super::poll_released;
    use super::poll_result;
    use super::publish_released;
    use super::publish_result;
    use crate::context::ContextOptions;
    use crate::context::OperationContext;
    use crate::report::OperationId;
    use crate::report::OperationOutcome;
    use crate::report::ResourcePhase;
    use crate::report::SettlementReport;

    struct Probe {
        shared: Arc<Shared<(), ()>>,
        lock_reentries: Arc<AtomicUsize>,
    }

    fn check_reentry(probe: &Probe) {
        if probe.shared.state.try_lock().is_err() {
            probe.lock_reentries.fetch_add(1, Ordering::SeqCst);
        }
    }

    unsafe fn clone_waker(data: *const ()) -> RawWaker {
        let probe = unsafe { Arc::<Probe>::from_raw(data.cast()) };
        check_reentry(&probe);
        let clone = Arc::clone(&probe);
        std::mem::forget(probe);
        RawWaker::new(Arc::into_raw(clone).cast(), &VTABLE)
    }

    unsafe fn wake_waker(data: *const ()) {
        let probe = unsafe { Arc::<Probe>::from_raw(data.cast()) };
        check_reentry(&probe);
        drop(probe);
    }

    unsafe fn wake_by_ref(data: *const ()) {
        let probe = unsafe { &*data.cast::<Probe>() };
        check_reentry(probe);
    }

    unsafe fn drop_waker(data: *const ()) {
        let probe = unsafe { Arc::<Probe>::from_raw(data.cast()) };
        check_reentry(&probe);
        drop(probe);
    }

    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone_waker, wake_waker, wake_by_ref, drop_waker);

    fn reentrant_waker(shared: &Arc<Shared<(), ()>>, lock_reentries: &Arc<AtomicUsize>) -> Waker {
        let probe = Arc::new(Probe {
            shared: Arc::clone(shared),
            lock_reentries: Arc::clone(lock_reentries),
        });
        let raw = RawWaker::new(Arc::into_raw(probe).cast(), &VTABLE);
        unsafe { Waker::from_raw(raw) }
    }

    fn shared() -> Arc<Shared<(), ()>> {
        Arc::new(Shared {
            state: Mutex::new(State {
                result: None,
                result_taken: false,
                result_discarded: false,
                released: None,
                result_waiters: Vec::new(),
                release_waiters: Vec::new(),
                result_lease: None,
                completion_guards: Vec::new(),
            }),
            changed: Condvar::new(),
            context: OperationContext::new(ContextOptions::default()),
        })
    }

    fn report() -> SettlementReport {
        SettlementReport {
            operation_id: OperationId::new(1),
            resource_phase: ResourcePhase::Released,
            all_released: true,
            children: Arc::from([]),
            cleanup_failures: Arc::from([]),
            transfers: Arc::from([]),
            unconfirmed_count: 0,
            effects: Arc::from([]),
            effect_overflow: 0,
        }
    }

    #[test]
    fn test_result_poll_returns_published_outcome_only_once() {
        let shared = shared();
        let mut cx = Context::from_waker(Waker::noop());
        publish_result(&shared, OperationOutcome::Succeeded(()));

        assert!(matches!(
            poll_result(&shared, &mut cx),
            Poll::Ready(Ok(OperationOutcome::Succeeded(())))
        ));
        assert!(matches!(
            poll_result(&shared, &mut cx),
            Poll::Ready(Err(crate::report::ObservationError::AlreadyTaken))
        ));
    }

    #[test]
    fn test_result_and_released_wakers_reenter_only_after_state_unlocks() {
        let shared = shared();
        let lock_reentries = Arc::new(AtomicUsize::new(0));
        let waker = reentrant_waker(&shared, &lock_reentries);
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(poll_result(&shared, &mut cx), Poll::Pending));
        publish_result(&shared, OperationOutcome::Succeeded(()));
        assert_eq!(lock_reentries.load(Ordering::SeqCst), 0);

        assert!(matches!(poll_released(&shared, &mut cx), Poll::Pending));
        publish_released(&shared, report());
        assert_eq!(lock_reentries.load(Ordering::SeqCst), 0);
    }
}

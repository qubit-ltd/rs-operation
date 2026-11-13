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
use std::task::Context;
use std::task::Poll;

use super::CleanupObservation;
use super::CleanupPanic;
use super::SchedulerState;
use super::internal::CleanupTaskState;
use crate::report::CleanupOutcome;
use crate::report::MonotonicTime;
use crate::report::ResourceId;

/// One driven cleanup future and its independent concurrency permit.
///
/// A task is produced by `CleanupScheduler::schedule` or reacquired from a
/// `CleanupResponsibility`; callers drive it with `poll_at`.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::CleanupObservation;
/// use qubit_operation::resource::CleanupScheduler;
/// use qubit_operation::report::CleanupOutcome;
/// use qubit_operation::report::ClockDomainId;
/// use qubit_operation::report::MonotonicTime;
/// use qubit_operation::report::OperationId;
/// use qubit_operation::report::ResourceId;
/// use std::task::Context;
/// use std::task::Poll;
/// use std::task::Waker;
///
/// let scheduler = CleanupScheduler::new(1);
/// let resource_id = ResourceId::new(OperationId::new(7), 0, 0);
/// let mut task = match scheduler.schedule(resource_id, Box::pin(async {
///     CleanupOutcome::Cleaned
/// })) {
///     Ok(task) => task,
///     Err(_) => panic!("the cleanup slot is available"),
/// };
/// let mut cx = Context::from_waker(Waker::noop());
/// let observation = task.poll_at(&mut cx, MonotonicTime::new(ClockDomainId(1), 0));
/// assert!(matches!(
///     observation,
///     Poll::Ready(CleanupObservation::Settled(Ok(CleanupOutcome::Cleaned)))
/// ));
/// ```
pub struct CleanupTask {
    /// Resource associated with the scheduled cleanup.
    pub(super) resource_id: ResourceId,
    /// Shared scheduler state used to remove the task after completion.
    pub(super) scheduler: Arc<SchedulerState>,
    /// Future, deadline, and permit state shared by reacquired observers.
    pub(super) state: Arc<CleanupTaskState>,
}

impl CleanupTask {
    /// Observe or drive cleanup at an injected monotonic time.
    ///
    /// The first observation at/after the deadline returns `Unconfirmed` and
    /// retains both future and permit. A later call continues polling so the
    /// eventual real outcome can still be confirmed.
    ///
    /// Multiple observers may call this method for the same retained task. If
    /// another observer is currently polling, this call returns the `Pending`
    /// observation. That observation does not register this caller's waker;
    /// callers needing progress should arrange to poll again.
    ///
    /// # Parameters
    ///
    /// * `cx` — task context used to register for future progress.
    /// * `now` — current monotonic time, which must share the deadline's
    ///   domain.
    ///
    /// # Returns
    ///
    /// Always returns `Ready` with the current observation; `Pending` is an
    /// observation value indicating that the underlying cleanup future remains
    /// active.
    pub fn poll_at(&mut self, cx: &mut Context<'_>, now: MonotonicTime) -> Poll<CleanupObservation> {
        #[cfg(test)]
        let gate = self
            .state
            .poll_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        #[cfg(test)]
        if let Some(gate) = gate {
            gate.wait();
        }
        let mut future = {
            let mut task = self.state.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if task.settled {
                return Poll::Ready(CleanupObservation::AlreadySettled {
                    resource_id: self.resource_id,
                });
            }
            if task.polling {
                return Poll::Ready(CleanupObservation::Pending);
            }
            if let Some(deadline) = task.deadline {
                if now.domain != deadline.domain {
                    return Poll::Ready(CleanupObservation::ClockDomainMismatch {
                        resource_id: self.resource_id,
                    });
                }
                if !task.unconfirmed_reported && now.ticks >= deadline.ticks {
                    task.unconfirmed_reported = true;
                    return Poll::Ready(CleanupObservation::Unconfirmed {
                        resource_id: self.resource_id,
                    });
                }
            }
            let future = task.future.take().expect("cleanup future is retained by its task");
            task.polling = true;
            future
        };
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx)));
        self.finish_poll(future, polled)
    }

    /// Convert a completed future poll into its public observation and settle
    /// its registration.
    ///
    /// The future is restored only for a pending poll; every completed or
    /// panicked poll follows the existing settlement path.
    ///
    /// # Parameters
    ///
    /// * `future` — future removed from shared state for this poll.
    /// * `polled` — result of catching a panic from the future's poll
    ///   operation.
    ///
    /// # Returns
    ///
    /// The pending observation or the settled outcome/panic fact.
    fn finish_poll(
        &mut self,
        future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>,
        polled: Result<Poll<CleanupOutcome>, Box<dyn std::any::Any + Send + 'static>>,
    ) -> Poll<CleanupObservation> {
        match polled {
            Ok(Poll::Pending) => {
                let mut task = self.state.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
                task.future = Some(future);
                task.polling = false;
                Poll::Ready(CleanupObservation::Pending)
            }
            Ok(Poll::Ready(outcome)) => {
                let drop_panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(future))).is_err();
                self.complete_task();
                if drop_panicked {
                    Poll::Ready(CleanupObservation::Settled(Err(CleanupPanic {
                        resource_id: self.resource_id,
                    })))
                } else {
                    Poll::Ready(CleanupObservation::Settled(Ok(outcome)))
                }
            }
            Err(_) => {
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(future)));
                self.complete_task();
                Poll::Ready(CleanupObservation::Settled(Err(CleanupPanic {
                    resource_id: self.resource_id,
                })))
            }
        }
    }

    /// Mark the task settled, release its permit, and remove its registry
    /// entry.
    fn complete_task(&self) {
        {
            let mut task = self.state.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            task.polling = false;
            task.settled = true;
            task.permit = None;
        }
        self.scheduler
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.resource_id);
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Waker;

    use super::super::internal::PollGate;
    use super::Poll;
    use crate::report::CleanupOutcome;
    use crate::report::ClockDomainId;
    use crate::report::MonotonicTime;
    use crate::report::OperationId;
    use crate::report::ResourceId;
    use crate::resource::CleanupObservation;
    use crate::resource::CleanupScheduler;

    struct ControlledPending {
        ready: Arc<AtomicBool>,
        active_polls: Arc<AtomicUsize>,
        max_polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl Future for ControlledPending {
        type Output = CleanupOutcome;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            let active = self.active_polls.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_polls.fetch_max(active, Ordering::SeqCst);
            let ready = self.ready.load(Ordering::SeqCst);
            self.active_polls.fetch_sub(1, Ordering::SeqCst);
            if ready {
                Poll::Ready(CleanupOutcome::Cleaned)
            } else {
                Poll::Pending
            }
        }
    }

    impl Drop for ControlledPending {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_observers_prevent_duplicate_cleanup_future_polling() {
        let scheduler = CleanupScheduler::new(1);
        let id = ResourceId::new(OperationId::new(721), 0, 0);
        let ready = Arc::new(AtomicBool::new(false));
        let active_polls = Arc::new(AtomicUsize::new(0));
        let max_polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send>> = Box::pin(ControlledPending {
            ready: Arc::clone(&ready),
            active_polls: Arc::clone(&active_polls),
            max_polls: Arc::clone(&max_polls),
            drops: Arc::clone(&drops),
        });
        let task = scheduler.schedule(id, future).unwrap_or_else(|_| unreachable!());
        let gate = Arc::new(PollGate::new());
        *task
            .state
            .poll_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::clone(&gate));
        let responsibility = scheduler.responsibility_owner();
        let mut first = task;
        let mut second = responsibility.task(id).expect("scheduled task remains registered");
        let mut final_observer = responsibility.task(id).expect("scheduled task remains registered");
        let mut settled_observer = responsibility.task(id).expect("scheduled task remains registered");
        let first_thread = std::thread::spawn(move || {
            let mut cx = std::task::Context::from_waker(Waker::noop());
            first.poll_at(&mut cx, MonotonicTime::new(ClockDomainId(1), 0))
        });
        let second_thread = std::thread::spawn(move || {
            let mut cx = std::task::Context::from_waker(Waker::noop());
            second.poll_at(&mut cx, MonotonicTime::new(ClockDomainId(1), 0))
        });
        gate.release_when_ready();
        let first = first_thread.join().expect("first observer must not panic");
        let second = second_thread.join().expect("second observer must not panic");
        assert!(matches!(first, Poll::Ready(CleanupObservation::Pending)));
        assert!(matches!(second, Poll::Ready(CleanupObservation::Pending)));
        assert_eq!(max_polls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        ready.store(true, Ordering::SeqCst);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            final_observer.poll_at(&mut cx, MonotonicTime::new(ClockDomainId(1), 1)),
            Poll::Ready(CleanupObservation::Settled(Ok(CleanupOutcome::Cleaned)))
        ));
        assert!(matches!(
            settled_observer.poll_at(&mut cx, MonotonicTime::new(ClockDomainId(1), 2)),
            Poll::Ready(CleanupObservation::AlreadySettled { resource_id }) if resource_id == id
        ));
        assert_eq!(scheduler.active(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_deadline_mismatch_and_unconfirmed_observation_preserve_task() {
        let scheduler = CleanupScheduler::new(1);
        let id = ResourceId::new(OperationId::new(722), 0, 0);
        let future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send>> = Box::pin(async { CleanupOutcome::Cleaned });
        let mut task = scheduler
            .schedule_until(id, MonotonicTime::new(ClockDomainId(7), 10), future)
            .unwrap_or_else(|_| unreachable!());
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(
            task.poll_at(&mut context, MonotonicTime::new(ClockDomainId(8), 10)),
            Poll::Ready(CleanupObservation::ClockDomainMismatch { resource_id }) if resource_id == id
        ));
        assert_eq!(scheduler.active(), 1);
        assert!(scheduler.task(id).is_some());
        assert!(matches!(
            task.poll_at(&mut context, MonotonicTime::new(ClockDomainId(7), 10)),
            Poll::Ready(CleanupObservation::Unconfirmed { resource_id }) if resource_id == id
        ));
        assert_eq!(scheduler.active(), 1);
        assert!(matches!(
            task.poll_at(&mut context, MonotonicTime::new(ClockDomainId(7), 11)),
            Poll::Ready(CleanupObservation::Settled(Ok(CleanupOutcome::Cleaned)))
        ));
        assert_eq!(scheduler.active(), 0);
        assert!(scheduler.task(id).is_none());
    }

    struct PanickingPoll;

    impl Future for PanickingPoll {
        type Output = CleanupOutcome;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            panic!("cleanup poll failed");
        }
    }

    #[test]
    fn test_panicking_cleanup_future_settles_as_cleanup_panic() {
        let scheduler = CleanupScheduler::new(1);
        let id = ResourceId::new(OperationId::new(723), 0, 0);
        let future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send>> = Box::pin(PanickingPoll);
        let mut task = scheduler.schedule(id, future).unwrap_or_else(|_| unreachable!());
        let mut context = Context::from_waker(Waker::noop());

        assert!(matches!(
            task.poll_at(&mut context, MonotonicTime::new(ClockDomainId(7), 0)),
            Poll::Ready(CleanupObservation::Settled(Err(_)))
        ));
        assert_eq!(scheduler.active(), 0);
        assert!(scheduler.task(id).is_none());
    }
}

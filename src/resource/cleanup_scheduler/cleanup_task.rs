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
        let deadline = {
            let task = self.state.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if task.settled {
                return Poll::Ready(CleanupObservation::AlreadySettled {
                    resource_id: self.resource_id,
                });
            }
            if task.polling {
                return Poll::Ready(CleanupObservation::Pending);
            }
            task.deadline
        };
        if let Some(deadline) = deadline {
            if now.domain != deadline.domain {
                return Poll::Ready(CleanupObservation::ClockDomainMismatch {
                    resource_id: self.resource_id,
                });
            }
            let mut task = self.state.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            if !task.unconfirmed_reported && now.ticks >= deadline.ticks {
                task.unconfirmed_reported = true;
                return Poll::Ready(CleanupObservation::Unconfirmed {
                    resource_id: self.resource_id,
                });
            }
        }
        let mut future = {
            let mut task = self.state.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            task.polling = true;
            task.future.take().expect("cleanup future is retained by its task")
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

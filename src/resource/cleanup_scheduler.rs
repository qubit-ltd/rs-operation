// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod cleanup_observation;
mod cleanup_panic;
mod cleanup_permit;
mod cleanup_responsibility;
mod cleanup_task;
mod internal;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

pub use self::cleanup_observation::CleanupObservation;
pub use self::cleanup_panic::CleanupPanic;
pub use self::cleanup_permit::CleanupPermit;
pub use self::cleanup_responsibility::CleanupResponsibility;
pub use self::cleanup_task::CleanupTask;
use self::internal::CleanupTaskInner;
use self::internal::CleanupTaskState;
use self::internal::SchedulerState;
use crate::report::CleanupOutcome;
use crate::report::MonotonicTime;
use crate::report::ResourceId;

/// Bounded cleanup concurrency, independent of business-operation admission.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::CleanupScheduler;
///
/// let scheduler = CleanupScheduler::new(2);
/// assert_eq!(scheduler.limit(), 2);
/// assert_eq!(scheduler.active(), 0);
/// ```
#[derive(Clone)]
pub struct CleanupScheduler {
    /// Shared state that enforces the pool's concurrency limit.
    state: Arc<SchedulerState>,
}

impl CleanupScheduler {
    /// Create an independent cleanup pool with a fixed concurrency limit.
    ///
    /// # Parameters
    ///
    /// * `limit` — maximum number of cleanup tasks that may hold permits.
    ///
    /// # Returns
    ///
    /// A scheduler with no active tasks. A zero limit disables immediate
    /// acquisition and causes permit polling to report that capacity is
    /// unavailable.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        Self {
            state: Arc::new(SchedulerState {
                limit,
                active: Arc::new(AtomicUsize::new(0)),
                capacity_waiters: Arc::new(Mutex::new(Vec::new())),
                tasks: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    /// Reserve one cleanup slot without waiting or consuming business capacity.
    ///
    /// # Returns
    ///
    /// `Some(permit)` when a slot is reserved, or `None` when the configured
    /// limit has already been reached.
    #[must_use]
    pub fn try_acquire(&self) -> Option<CleanupPermit> {
        let mut active = self.state.active.load(Ordering::Acquire);
        loop {
            if active >= self.state.limit {
                return None;
            }
            match self
                .state
                .active
                .compare_exchange_weak(active, active + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Some(CleanupPermit {
                        active: Arc::clone(&self.state.active),
                        capacity_waiters: Arc::clone(&self.state.capacity_waiters),
                    });
                }
                Err(observed) => active = observed,
            }
        }
    }

    /// Return the number of currently reserved cleanup slots.
    ///
    /// # Returns
    ///
    /// The number of permits currently held by callers and active tasks.
    #[must_use]
    #[inline]
    pub fn active(&self) -> usize {
        self.state.active.load(Ordering::Acquire)
    }

    /// Return the configured concurrency limit.
    ///
    /// # Returns
    ///
    /// The maximum number of simultaneously held cleanup permits.
    #[must_use]
    #[inline]
    pub fn limit(&self) -> usize {
        self.state.limit
    }

    /// Transfer an owning reference to the supervisor/driver lifecycle.
    ///
    /// # Returns
    ///
    /// A cloneable owner that keeps scheduled tasks alive independently of
    /// scheduler observers.
    #[must_use]
    #[inline]
    pub fn responsibility_owner(&self) -> CleanupResponsibility {
        CleanupResponsibility {
            state: Arc::clone(&self.state),
        }
    }

    /// Reacquire an observer for a cleanup task retained by this scheduler.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — identifier of the retained task to observe.
    ///
    /// # Returns
    ///
    /// `Some(task)` while the task is registered, or `None` after it settles or
    /// when no task has that identifier.
    #[must_use]
    #[inline]
    pub fn task(&self, resource_id: ResourceId) -> Option<CleanupTask> {
        self.responsibility_owner().task(resource_id)
    }

    /// Schedule a cleanup future if a cleanup slot is available.
    ///
    /// The future is polled outside scheduler locks. Panics are converted to a
    /// `CleanupPanic` fact; a separately registered recovery capsule remains
    /// owned by its recovery registry. This cannot restore a consumed adapter.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — stable identifier used to reacquire this task.
    /// * `future` — owned cleanup operation, returned unchanged if no slot is
    ///   available or a task with this identifier is already registered.
    ///
    /// # Returns
    ///
    /// The registered task on success, or the original future when scheduling
    /// cannot proceed.
    ///
    /// # Errors
    ///
    /// Returns the original future if capacity is full or the resource ID is
    /// already registered.
    pub fn schedule(
        &self,
        resource_id: ResourceId,
        future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>,
    ) -> Result<CleanupTask, Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>> {
        let Some(permit) = self.try_acquire() else {
            return Err(future);
        };
        self.schedule_with_permit(resource_id, permit, future)
    }

    /// Schedule a cleanup task with a clock-domain-bound observation deadline.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — stable identifier used to reacquire this task.
    /// * `deadline` — observation threshold in the same monotonic clock domain
    ///   supplied to `CleanupTask::poll_at`.
    /// * `future` — owned cleanup operation, returned if registration fails.
    ///
    /// # Returns
    ///
    /// The registered task, or the original future if scheduling fails.
    ///
    /// # Errors
    ///
    /// Returns the original future if capacity is full or the resource ID is
    /// already registered.
    pub fn schedule_until(
        &self,
        resource_id: ResourceId,
        deadline: MonotonicTime,
        future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>,
    ) -> Result<CleanupTask, Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>> {
        let task = self.schedule(resource_id, future)?;
        task.state
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .deadline = Some(deadline);
        Ok(task)
    }

    /// Register a future using a permit already reserved by an admission path.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — identifier assigned to the scheduled cleanup.
    /// * `permit` — reserved cleanup slot transferred to the new task.
    /// * `future` — owned cleanup operation to register.
    ///
    /// # Returns
    ///
    /// The task on success; the original future if the identifier is already
    /// registered. In that case the supplied permit is released.
    ///
    /// # Errors
    ///
    /// Returns the original future if the identifier is already registered;
    /// the supplied permit is released on that path.
    pub(crate) fn schedule_with_permit(
        &self,
        resource_id: ResourceId,
        permit: CleanupPermit,
        future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>,
    ) -> Result<CleanupTask, Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>> {
        let mut tasks = self.state.tasks.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if tasks.contains_key(&resource_id) {
            drop(permit);
            return Err(future);
        }
        let state = Arc::new(CleanupTaskState {
            inner: Mutex::new(CleanupTaskInner {
                deadline: None,
                unconfirmed_reported: false,
                future: Some(future),
                permit: Some(permit),
                polling: false,
                settled: false,
            }),
        });
        tasks.insert(resource_id, Arc::clone(&state));
        drop(tasks);
        Ok(CleanupTask {
            resource_id,
            scheduler: Arc::clone(&self.state),
            state,
        })
    }

    /// Poll for cleanup capacity and register the current task when it is full.
    ///
    /// # Parameters
    ///
    /// * `cx` — context whose waker is notified when a permit is released.
    ///
    /// # Returns
    ///
    /// `Ready(Some(permit))` when capacity is acquired, `Ready(None)` for a
    /// zero-capacity scheduler, or `Pending` while waiting for a permit.
    pub(crate) fn poll_acquire(&self, cx: &mut Context<'_>) -> Poll<Option<CleanupPermit>> {
        if let Some(permit) = self.try_acquire() {
            return Poll::Ready(Some(permit));
        }
        if self.state.limit == 0 {
            return Poll::Ready(None);
        }
        let mut candidate = Some(cx.waker().clone());
        let inserted = {
            let mut waiters = self
                .state
                .capacity_waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if waiters.iter().any(|waker| waker.will_wake(cx.waker())) {
                false
            } else {
                waiters.push(candidate.take().expect("candidate is present"));
                true
            }
        };
        if !inserted {
            drop(candidate);
        }
        if let Some(permit) = self.try_acquire() {
            self.remove_capacity_waiter(cx.waker());
            Poll::Ready(Some(permit))
        } else {
            Poll::Pending
        }
    }

    /// Remove a previously registered waiter after capacity is acquired.
    ///
    /// # Parameters
    ///
    /// * `waker` — waiter identity to remove from the capacity queue.
    fn remove_capacity_waiter(&self, waker: &Waker) {
        let removed = {
            let mut waiters = self
                .state
                .capacity_waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut kept = Vec::with_capacity(waiters.len());
            let mut removed = Vec::new();
            for waiter in waiters.drain(..) {
                if waiter.will_wake(waker) {
                    removed.push(waiter);
                } else {
                    kept.push(waiter);
                }
            }
            *waiters = kept;
            removed
        };
        drop(removed);
    }
}

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::report::{CleanupOutcome, MonotonicTime};

/// Bounded cleanup concurrency, independent of business-operation admission.
#[derive(Clone)]
pub struct CleanupScheduler {
    state: Arc<SchedulerState>,
}

struct SchedulerState {
    limit: usize,
    active: Arc<AtomicUsize>,
    tasks: Mutex<BTreeMap<crate::report::ResourceId, Arc<CleanupTaskState>>>,
}

/// One cleanup concurrency slot. Dropping it returns the slot.
pub struct CleanupPermit {
    active: Arc<AtomicUsize>,
}

/// Panic fact for a cleanup future whose independent recovery capsule remains registered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CleanupPanic {
    resource_id: crate::report::ResourceId,
}

impl CleanupPanic {
    /// Resource whose cleanup future panicked.
    pub const fn resource_id(self) -> crate::report::ResourceId {
        self.resource_id
    }
}

/// One driven cleanup future and its independent concurrency permit.
pub struct CleanupTask {
    resource_id: crate::report::ResourceId,
    scheduler: Arc<SchedulerState>,
    state: Arc<CleanupTaskState>,
}

struct CleanupTaskState {
    inner: Mutex<CleanupTaskInner>,
}

struct CleanupTaskInner {
    deadline: Option<MonotonicTime>,
    unconfirmed_reported: bool,
    future: Option<Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>>,
    permit: Option<CleanupPermit>,
    polling: bool,
    settled: bool,
}

/// Observation of a driven cleanup task; `Unconfirmed` leaves it alive.
#[derive(Debug)]
pub enum CleanupObservation {
    Pending,
    AlreadySettled {
        resource_id: crate::report::ResourceId,
    },
    Unconfirmed {
        resource_id: crate::report::ResourceId,
    },
    ClockDomainMismatch {
        resource_id: crate::report::ResourceId,
    },
    Settled(Result<CleanupOutcome, CleanupPanic>),
}

impl CleanupScheduler {
    /// Create an independent cleanup pool with a fixed concurrency limit.
    pub fn new(limit: usize) -> Self {
        Self {
            state: Arc::new(SchedulerState {
                limit,
                active: Arc::new(AtomicUsize::new(0)),
                tasks: Mutex::new(BTreeMap::new()),
            }),
        }
    }

    /// Reserve one cleanup slot without waiting or consuming business capacity.
    pub fn try_acquire(&self) -> Option<CleanupPermit> {
        let mut active = self.state.active.load(Ordering::Acquire);
        loop {
            if active >= self.state.limit {
                return None;
            }
            match self.state.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(CleanupPermit {
                        active: Arc::clone(&self.state.active),
                    });
                }
                Err(observed) => active = observed,
            }
        }
    }

    /// Return the number of currently reserved cleanup slots.
    pub fn active(&self) -> usize {
        self.state.active.load(Ordering::Acquire)
    }

    /// Return the configured concurrency limit.
    pub fn limit(&self) -> usize {
        self.state.limit
    }

    /// Schedule a cleanup future if a cleanup slot is available.
    ///
    /// The future is polled outside scheduler locks. Panics are converted to a
    /// `CleanupPanic` fact; a separately registered recovery capsule remains
    /// owned by its recovery registry. This cannot restore a consumed adapter.
    pub fn schedule(
        &self,
        resource_id: crate::report::ResourceId,
        future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>,
    ) -> Result<CleanupTask, Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>> {
        let Some(permit) = self.try_acquire() else {
            return Err(future);
        };
        let mut tasks = self
            .state
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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

    /// Schedule a cleanup task with a clock-domain-bound observation deadline.
    pub fn schedule_until(
        &self,
        resource_id: crate::report::ResourceId,
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

    /// Reacquire an observer for a cleanup task retained by this scheduler.
    pub fn task(&self, resource_id: crate::report::ResourceId) -> Option<CleanupTask> {
        let state = self
            .state
            .tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&resource_id)
            .cloned()?;
        Some(CleanupTask {
            resource_id,
            scheduler: Arc::clone(&self.state),
            state,
        })
    }
}

impl Drop for CleanupPermit {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl CleanupTask {
    /// Observe or drive cleanup at an injected monotonic time.
    ///
    /// The first observation at/after the deadline returns `Unconfirmed` and
    /// retains both future and permit. A later call continues polling so the
    /// eventual real outcome can still be confirmed.
    pub fn poll_at(
        &mut self,
        cx: &mut Context<'_>,
        now: MonotonicTime,
    ) -> Poll<CleanupObservation> {
        let deadline = {
            let task = self
                .state
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
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
            let mut task = self
                .state
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !task.unconfirmed_reported && now.ticks >= deadline.ticks {
                task.unconfirmed_reported = true;
                return Poll::Ready(CleanupObservation::Unconfirmed {
                    resource_id: self.resource_id,
                });
            }
        }
        let mut future = {
            let mut task = self
                .state
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            task.polling = true;
            task.future
                .take()
                .expect("cleanup future is retained by its task")
        };
        let polled =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(cx)));
        match polled {
            Ok(Poll::Pending) => {
                let mut task = self
                    .state
                    .inner
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                task.future = Some(future);
                task.polling = false;
                Poll::Ready(CleanupObservation::Pending)
            }
            Ok(Poll::Ready(outcome)) => {
                let drop_panicked =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(future)))
                        .is_err();
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

    fn complete_task(&self) {
        {
            let mut task = self
                .state
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
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

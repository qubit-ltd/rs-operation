// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::CleanupTask;
use super::SchedulerState;
use crate::report::ResourceId;

/// Durable owner of active cleanup tasks for a supervisor or driver.
///
/// Keep this value for the lifetime of the managed operation. Dropping a
/// `CleanupScheduler` or an observer does not cancel tasks while this owner is
/// retained. The responsibility owner deliberately starts no thread/runtime;
/// the supervisor reacquires task observers and drives them explicitly.
///
/// # Examples
///
/// ```
/// use qubit_operation::resource::CleanupScheduler;
///
/// let scheduler = CleanupScheduler::new(1);
/// let owner = scheduler.responsibility_owner();
/// assert_eq!(owner.limit(), 1);
/// ```
#[derive(Clone)]
pub struct CleanupResponsibility {
    /// State retained for the lifetime of the managed operation.
    pub(super) state: Arc<SchedulerState>,
}

impl CleanupResponsibility {
    /// Number of cleanup permits still retained by active tasks.
    ///
    /// # Returns
    ///
    /// The current number of reserved cleanup slots.
    #[must_use]
    #[inline]
    pub fn active(&self) -> usize {
        self.state.active.load(Ordering::Acquire)
    }

    /// Configured cleanup concurrency limit.
    ///
    /// # Returns
    ///
    /// The maximum number of simultaneously reserved cleanup slots.
    #[must_use]
    #[inline]
    pub fn limit(&self) -> usize {
        self.state.limit
    }

    /// Reacquire an observer while keeping task and permit owned here.
    ///
    /// # Parameters
    ///
    /// * `resource_id` — identifier of the retained task to observe.
    ///
    /// # Returns
    ///
    /// `Some(task)` while registered, otherwise `None`.
    #[must_use]
    pub fn task(&self, resource_id: ResourceId) -> Option<CleanupTask> {
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

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use crate::report::CleanupOutcome;
    use crate::report::ClockDomainId;
    use crate::report::MonotonicTime;
    use crate::report::OperationId;
    use crate::report::ResourceId;
    use crate::resource::CleanupObservation;
    use crate::resource::CleanupScheduler;

    #[test]
    fn test_responsibility_reacquires_task_after_scheduler_drop() {
        let resource_id = ResourceId::new(OperationId::new(991), 2, 3);
        let responsibility = {
            let scheduler = CleanupScheduler::new(1);
            let future: Pin<Box<dyn Future<Output = CleanupOutcome> + Send>> =
                Box::pin(async { CleanupOutcome::Cleaned });
            let task = scheduler
                .schedule(resource_id, future)
                .unwrap_or_else(|_| unreachable!());
            drop(task);
            scheduler.responsibility_owner()
        };

        assert_eq!(responsibility.limit(), 1);
        assert_eq!(responsibility.active(), 1);
        assert!(
            responsibility
                .task(ResourceId::new(OperationId::new(992), 0, 0))
                .is_none()
        );

        let mut observer = responsibility.task(resource_id).expect("owner retains scheduled task");
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(
            observer.poll_at(&mut context, MonotonicTime::new(ClockDomainId(1), 0)),
            Poll::Ready(CleanupObservation::Settled(Ok(CleanupOutcome::Cleaned)))
        ));
        assert_eq!(responsibility.active(), 0);
        assert!(responsibility.task(resource_id).is_none());
    }
}

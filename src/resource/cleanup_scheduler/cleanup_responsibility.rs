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

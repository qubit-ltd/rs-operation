// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use super::cleanup_dispatcher_error::CleanupDispatcherError;
use super::cleanup_pump::CleanupPump;
use super::internal::CleanupDispatcherInner;
use super::internal::DispatchPermit;
use super::internal::QueueState;
use super::lock;
use crate::v02::executor::CleanupExecutor;
use crate::v02::resource::cleanup_port::CleanupDispatch;
use crate::v02::resource::cleanup_port::CleanupDispatchPermit;
use crate::v02::resource::cleanup_port::CleanupQueueError;

/// Bounded queue that hands cleanup jobs to the cleanup executor.
///
/// Reservations cover resources awaiting cleanup and jobs still queued. A
/// reservation is released when its job is accepted by the executor or when
/// an uncommitted permit is dropped.
pub(crate) struct CleanupDispatcher {
    /// Shared queue and executor state used by reservations and the pump.
    inner: Arc<CleanupDispatcherInner>,
}

impl CleanupDispatcher {
    /// Creates a dispatcher with no background pump.
    ///
    /// # Parameters
    ///
    /// - `scope_resource_capacity`: Maximum number of simultaneous resource
    ///   reservations and queued jobs for this dispatcher. Must be nonzero.
    /// - `executor`: Bounded executor that accepts jobs drained from the queue.
    ///
    /// # Returns
    ///
    /// A dispatcher with open admission.
    ///
    /// # Errors
    ///
    /// Returns [`CleanupDispatcherError::ZeroCapacity`] when capacity is zero.
    pub(crate) fn new(
        scope_resource_capacity: usize,
        executor: Arc<CleanupExecutor>,
    ) -> Result<Self, CleanupDispatcherError> {
        Self::new_inner(scope_resource_capacity, executor, None)
    }

    /// Creates a dispatcher whose pump can wake it when jobs are queued.
    ///
    /// # Parameters
    ///
    /// - `scope_resource_capacity`: Maximum number of simultaneous resource
    ///   reservations and queued jobs for this dispatcher. Must be nonzero.
    /// - `executor`: Bounded executor that accepts jobs drained from the queue.
    /// - `pump`: Background pump registered when the queue becomes nonempty.
    ///
    /// # Returns
    ///
    /// A dispatcher with open admission and the supplied pump.
    ///
    /// # Errors
    ///
    /// Returns [`CleanupDispatcherError::ZeroCapacity`] when capacity is zero.
    pub(crate) fn new_with_pump(
        scope_resource_capacity: usize,
        executor: Arc<CleanupExecutor>,
        pump: Arc<CleanupPump>,
    ) -> Result<Self, CleanupDispatcherError> {
        Self::new_inner(scope_resource_capacity, executor, Some(pump))
    }

    /// Builds dispatcher state shared by both constructors.
    ///
    /// # Parameters
    ///
    /// - `scope_resource_capacity`: Nonzero bound for reservations and queued
    ///   cleanup jobs.
    /// - `executor`: Executor used to accept cleanup jobs.
    /// - `pump`: Optional background pump for pending work.
    ///
    /// # Returns
    ///
    /// A dispatcher with initialized admission and queue state.
    ///
    /// # Errors
    ///
    /// Returns [`CleanupDispatcherError::ZeroCapacity`] when the capacity is
    /// zero.
    fn new_inner(
        scope_resource_capacity: usize,
        executor: Arc<CleanupExecutor>,
        pump: Option<Arc<CleanupPump>>,
    ) -> Result<Self, CleanupDispatcherError> {
        if scope_resource_capacity == 0 {
            return Err(CleanupDispatcherError::ZeroCapacity);
        }
        Ok(Self {
            inner: Arc::new(CleanupDispatcherInner {
                capacity: scope_resource_capacity,
                executor,
                state: Mutex::new(QueueState {
                    accepting: true,
                    reserved: 0,
                    pending: VecDeque::with_capacity(scope_resource_capacity),
                }),
                driver: Mutex::new(()),
                pump,
                pump_registration: Mutex::new(None),
            }),
        })
    }

    /// Schedules up to `limit` pending jobs. If the executor has no slot, the
    /// first rejected job remains at the front of the queue with its resource
    /// owner intact. This method never blocks or invokes user cleanup inline.
    ///
    /// # Parameters
    ///
    /// - `self`: The dispatcher whose pending jobs are offered to the executor.
    /// - `limit`: Maximum number of jobs to schedule in this call.
    ///
    /// # Returns
    ///
    /// The number of jobs accepted by the executor.
    pub(crate) fn drive(&self, limit: usize) -> usize {
        Self::drive_inner(&self.inner, limit)
    }

    /// Drains pending jobs while holding the single-driver gate.
    ///
    /// Rejected jobs return to the front of the queue, preserving FIFO order
    /// and their resource ownership.
    ///
    /// # Parameters
    ///
    /// - `inner`: Shared dispatcher state to drain.
    /// - `limit`: Maximum number of jobs to offer to the executor.
    ///
    /// # Returns
    ///
    /// The number of jobs accepted by the executor.
    pub(in crate::v02::resource::cleanup) fn drive_inner(inner: &Arc<CleanupDispatcherInner>, limit: usize) -> usize {
        let _driver = lock(&inner.driver);
        let mut scheduled = 0;
        while scheduled < limit {
            let Some(job) = lock(&inner.state).pending.pop_front() else {
                break;
            };
            match job.schedule(Arc::clone(&inner.executor)) {
                Ok(()) => {
                    let mut state = lock(&inner.state);
                    debug_assert!(state.reserved > 0);
                    state.reserved -= 1;
                    scheduled += 1;
                }
                Err(job) => {
                    lock(&inner.state).pending.push_front(job);
                    break;
                }
            }
        }
        if lock(&inner.state).pending.is_empty()
            && let Some(pump) = &inner.pump
        {
            pump.unregister(inner);
        }
        scheduled
    }

    /// Stops new scope registrations. Previously reserved permits remain
    /// valid and can enqueue after close, and `drive` continues to drain them.
    ///
    /// # Parameters
    ///
    /// - `self`: The dispatcher whose admission is being closed.
    pub(crate) fn close_admission(&self) {
        lock(&self.inner.state).accepting = false;
    }

    /// Reports how many jobs are waiting for executor capacity.
    ///
    /// # Parameters
    ///
    /// - `self`: The dispatcher whose pending queue is inspected.
    ///
    /// # Returns
    ///
    /// The current number of queued jobs.
    #[must_use = "the pending cleanup job count should be inspected"]
    pub(crate) fn pending_count(&self) -> usize {
        lock(&self.inner.state).pending.len()
    }

    /// Reports outstanding reservations, including jobs still queued.
    ///
    /// Accepted executor jobs no longer consume this queue reservation.
    ///
    /// # Parameters
    ///
    /// - `self`: The dispatcher whose reservation count is inspected.
    ///
    /// # Returns
    ///
    /// The number of active resource reservations and queued-job reservations.
    #[must_use = "the reservation count should be inspected"]
    pub(crate) fn reserved_count(&self) -> usize {
        lock(&self.inner.state).reserved
    }

    /// Returns the configured bound for reservations and queued jobs.
    ///
    /// # Parameters
    ///
    /// - `self`: The dispatcher whose capacity is queried.
    ///
    /// # Returns
    ///
    /// The nonzero per-dispatcher queue capacity.
    #[must_use = "the dispatcher capacity should be inspected"]
    pub(crate) fn capacity(&self) -> usize {
        self.inner.capacity
    }
}

impl CleanupDispatch for CleanupDispatcher {
    /// Reserves one queue slot before a cleanup owner is enqueued.
    ///
    /// # Parameters
    ///
    /// - `self`: The dispatcher admitting the cleanup owner.
    ///
    /// # Returns
    ///
    /// A permit that can enqueue one job, or an error if admission is closed or
    /// the reservation bound is full.
    ///
    /// # Errors
    ///
    /// Returns [`CleanupQueueError::Closed`] after admission closes, or
    /// [`CleanupQueueError::Full`] when all queue reservations are in use.
    fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
        let mut state = lock(&self.inner.state);
        if !state.accepting {
            return Err(CleanupQueueError::Closed);
        }
        if state.reserved >= self.inner.capacity {
            return Err(CleanupQueueError::Full);
        }
        state.reserved += 1;
        Ok(Box::new(DispatchPermit {
            inner: Arc::clone(&self.inner),
            active: true,
        }))
    }
}

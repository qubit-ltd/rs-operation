// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashSet;
use std::collections::VecDeque;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use super::cleanup_dispatcher_error::CleanupDispatcherError;
use super::cleanup_transfer_error::CleanupTransferError;
use super::internal::TransferQueueState;
use super::lock;
use crate::v02::executor::CleanupExecutor;
use crate::v02::report::ManagerId;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::TransferReceipt;
use crate::v02::resource::cleanup_port::CleanupJob;

static NEXT_CLEANUP_TRANSFER_NONCE: AtomicU64 = AtomicU64::new(1);

/// Receives cleanup ownership transfers for one resource scope.
///
/// It validates manager/scope identity, capacity and duplicate identities
/// before accepting a job. Receipt callbacks run only after the owner is in
/// receiver custody and outside the queue lock.
/// Dropping the receiver before it drains deliberately forgets pending jobs to
/// avoid dropping resources whose transfer was already accepted.
pub(crate) struct CleanupTransferReceiver {
    /// Manager identity required for accepted resource transfers.
    manager: ManagerId,
    /// Scope that receives accepted cleanup jobs.
    scope: ResourceScopeId,
    /// Bound for reserved and pending cleanup transfers.
    capacity: usize,
    /// Executor that schedules accepted cleanup jobs.
    executor: Arc<CleanupExecutor>,
    /// Queue, admission state and identities currently held by the receiver.
    state: Mutex<TransferQueueState>,
    /// Serializes attempts to drain the pending queue.
    driver: Mutex<()>,
    /// Optional observer called after a receipt has been issued.
    receipt_callback: Option<Arc<dyn Fn(TransferReceipt) + Send + Sync + 'static>>,
}

impl Drop for CleanupTransferReceiver {
    /// Preserves accepted cleanup owners if the receiver is dropped
    /// prematurely.
    ///
    /// # Parameters
    ///
    /// - `self`: The receiver whose pending accepted jobs must not be dropped.
    fn drop(&mut self) {
        let state = self.state.get_mut().unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, job) in state.pending.drain(..) {
            // Accepted work must remain owned by a live manager/receiver.
            // Preserve it if the receiver is dropped prematurely.
            std::mem::forget(job);
        }
    }
}

impl CleanupTransferReceiver {
    /// Creates a receiver without a receipt observer.
    ///
    /// # Parameters
    ///
    /// - `manager`: Manager identity accepted by this receiver.
    /// - `scope`: Destination scope identity.
    /// - `capacity`: Positive bound for outstanding transfer reservations.
    /// - `executor`: Executor used to schedule accepted cleanup jobs.
    ///
    /// # Returns
    ///
    /// An open receiver for the specified manager and scope.
    ///
    /// # Errors
    ///
    /// Returns [`CleanupDispatcherError::ZeroCapacity`] when `capacity` is
    /// zero.
    pub(crate) fn new(
        manager: ManagerId,
        scope: ResourceScopeId,
        capacity: usize,
        executor: Arc<CleanupExecutor>,
    ) -> Result<Self, CleanupDispatcherError> {
        Self::new_inner(manager, scope, capacity, executor, None)
    }

    /// Creates a receiver with an observer called after each accepted transfer.
    ///
    /// Observer panics are caught and do not undo receiver custody or revoke a
    /// receipt that has already been issued.
    ///
    /// # Parameters
    ///
    /// - `manager`: Manager identity accepted by this receiver.
    /// - `scope`: Destination scope identity.
    /// - `capacity`: Positive bound for outstanding transfer reservations.
    /// - `executor`: Executor used to schedule accepted cleanup jobs.
    /// - `callback`: Observer invoked after acceptance and outside the queue
    ///   lock.
    ///
    /// # Returns
    ///
    /// An open receiver configured with the observer.
    ///
    /// # Errors
    ///
    /// Returns [`CleanupDispatcherError::ZeroCapacity`] when `capacity` is
    /// zero.
    pub(crate) fn new_with_receipt_callback(
        manager: ManagerId,
        scope: ResourceScopeId,
        capacity: usize,
        executor: Arc<CleanupExecutor>,
        callback: impl Fn(TransferReceipt) + Send + Sync + 'static,
    ) -> Result<Self, CleanupDispatcherError> {
        Self::new_inner(manager, scope, capacity, executor, Some(Arc::new(callback)))
    }

    /// Initializes receiver state shared by both constructors.
    ///
    /// # Parameters
    ///
    /// - `manager`: Accepted manager identity.
    /// - `scope`: Destination scope identity.
    /// - `capacity`: Positive reservation bound.
    /// - `executor`: Executor that will schedule accepted jobs.
    /// - `receipt_callback`: Optional post-acceptance observer.
    ///
    /// # Returns
    ///
    /// An initialized receiver with open admission.
    ///
    /// # Errors
    ///
    /// Returns [`CleanupDispatcherError::ZeroCapacity`] when `capacity` is
    /// zero.
    fn new_inner(
        manager: ManagerId,
        scope: ResourceScopeId,
        capacity: usize,
        executor: Arc<CleanupExecutor>,
        receipt_callback: Option<Arc<dyn Fn(TransferReceipt) + Send + Sync + 'static>>,
    ) -> Result<Self, CleanupDispatcherError> {
        if capacity == 0 {
            return Err(CleanupDispatcherError::ZeroCapacity);
        }
        Ok(Self {
            manager,
            scope,
            capacity,
            executor,
            state: Mutex::new(TransferQueueState {
                accepting: true,
                reserved: 0,
                pending: VecDeque::with_capacity(capacity),
                ids: HashSet::with_capacity(capacity),
            }),
            driver: Mutex::new(()),
            receipt_callback,
        })
    }

    /// Accepts a cleanup job and issues a receipt after storing the owner.
    ///
    /// On rejection, returns the original job with the error so the sender can
    /// keep ownership or retry elsewhere. A successful receipt means the
    /// receiver has already recorded both the resource identity and job.
    ///
    /// # Parameters
    ///
    /// - `self`: The destination receiver.
    /// - `resource`: Identity of the resource whose cleanup owner is moving.
    /// - `source_scope`: Scope from which the cleanup owner is transferred.
    /// - `job`: Cleanup action that remains owned by the receiver after
    ///   acceptance.
    ///
    /// # Returns
    ///
    /// A receipt after atomic acceptance, or the validation error together with
    /// the unaccepted job.
    pub(crate) fn accept(
        &self,
        resource: ResourceId,
        source_scope: ResourceScopeId,
        job: Box<dyn CleanupJob>,
    ) -> Result<TransferReceipt, (CleanupTransferError, Box<dyn CleanupJob>)> {
        if resource.manager() != self.manager {
            return Err((CleanupTransferError::WrongManager, job));
        }
        if source_scope == self.scope {
            return Err((CleanupTransferError::SameScope, job));
        }
        let mut state = lock(&self.state);
        if !state.accepting {
            return Err((CleanupTransferError::Closed, job));
        }
        if state.reserved >= self.capacity {
            return Err((CleanupTransferError::Full, job));
        }
        if state.ids.contains(&resource) {
            return Err((CleanupTransferError::Duplicate, job));
        }
        // The identity and owner enter receiver custody atomically before a
        // receipt can be created.
        state.ids.insert(resource);
        state.pending.push_back((resource, job));
        state.reserved += 1;
        let nonce = NEXT_CLEANUP_TRANSFER_NONCE.fetch_add(1, Ordering::Relaxed);
        let receipt = TransferReceipt::new(resource, source_scope, self.scope, nonce);
        drop(state);
        if let Some(callback) = &self.receipt_callback {
            let receipt_for_callback = receipt.clone();
            let _ = panic::catch_unwind(AssertUnwindSafe(|| callback(receipt_for_callback)));
        }
        Ok(receipt)
    }

    /// Schedules up to `limit` accepted jobs on the executor.
    ///
    /// If the executor is saturated, the first rejected job returns to the
    /// front of the queue with its reservation and identity intact.
    ///
    /// # Parameters
    ///
    /// - `self`: The receiver whose pending jobs are drained.
    /// - `limit`: Maximum number of jobs to offer to the executor.
    ///
    /// # Returns
    ///
    /// The number of jobs accepted by the executor.
    pub(crate) fn drive(&self, limit: usize) -> usize {
        let _driver = lock(&self.driver);
        let mut scheduled = 0;
        while scheduled < limit {
            let Some((resource, job)) = lock(&self.state).pending.pop_front() else {
                break;
            };
            match job.schedule(Arc::clone(&self.executor)) {
                Ok(()) => {
                    let mut state = lock(&self.state);
                    state.ids.remove(&resource);
                    state.reserved -= 1;
                    scheduled += 1;
                }
                Err(job) => {
                    lock(&self.state).pending.push_front((resource, job));
                    break;
                }
            }
        }
        scheduled
    }

    /// Stops accepting new transfers while preserving queued work for draining.
    ///
    /// # Parameters
    ///
    /// - `self`: The receiver whose admission is closed.
    pub(crate) fn close_admission(&self) {
        lock(&self.state).accepting = false;
    }

    /// Checks whether this receiver currently retains a resource identity.
    ///
    /// The identity is removed after the executor accepts its queued cleanup
    /// job; a receipt records the transfer but does not keep this query true.
    ///
    /// # Parameters
    ///
    /// - `self`: The receiver being queried.
    /// - `resource`: Resource identity to look up.
    ///
    /// # Returns
    ///
    /// `true` while the receiver has the resource's pending cleanup owner.
    pub(crate) fn contains(&self, resource: ResourceId) -> bool {
        lock(&self.state).ids.contains(&resource)
    }
}

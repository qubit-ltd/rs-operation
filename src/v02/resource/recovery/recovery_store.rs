// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::JoinHandle;

use crate::v02::internal::notify::Notify;
use crate::v02::report::RecoveryId;
use crate::v02::report::RecoveryPhase;
use crate::v02::report::RecoveryStoreCreateError;
use crate::v02::report::RecoveryStoreId;
use crate::v02::report::RecoveryStoreOptions;
use crate::v02::report::RecoveryStoreSnapshot;
use crate::v02::report::RecoveryTransfer;
use crate::v02::report::RecoveryTransferReceipt;
use crate::v02::resource::recovery::RecoveryAttempt;
use crate::v02::resource::recovery::RecoveryCore;
use crate::v02::resource::recovery::RecoveryStoreError;
use crate::v02::resource::recovery::RecoveryStoreShutdownHandle;
use crate::v02::resource::recovery::RecoveryStoreShutdownState;
use crate::v02::resource::recovery::RecoveryStoreState;
use crate::v02::resource::recovery::lock;
use crate::v02::resource::recovery::next_transfer_nonce;
use crate::v02::resource::recovery::store_usage;

#[cfg(test)]
pub(super) type CallbackInstallGate = Arc<Mutex<Option<Arc<(std::sync::Barrier, std::sync::Barrier)>>>>;

/// Bounded receiver that owns recovery groups after they leave a resource
/// scope.
///
/// The store limits both retained dependency groups and the total number of
/// resources across those groups. It retries cleanup on its worker pool and
/// keeps unresolved owners alive until cleanup succeeds or the group is moved
/// to another store. Clones refer to the same store state.
///
/// # Examples
///
/// ```
/// use qubit_operation::RecoveryStore;
/// use qubit_operation::RecoveryStoreOptions;
///
/// let store = RecoveryStore::new(RecoveryStoreOptions::default())?;
/// assert!(store.snapshot().accepting);
/// drop(
///     store
///         .request_shutdown()
///         .wait()
///         .expect("recovery store shutdown succeeds"),
/// );
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone)]
pub struct RecoveryStore {
    /// Stable identity used to identify this receiving store.
    pub(in crate::v02::resource::recovery) id: RecoveryStoreId,
    /// Maximum number of unresolved groups this store retains.
    pub(in crate::v02::resource::recovery) group_capacity: usize,
    /// Maximum total number of resources retained across unresolved groups.
    pub(in crate::v02::resource::recovery) resource_capacity: usize,
    /// Shared admission state and retained recovery groups.
    pub(in crate::v02::resource::recovery) state: Arc<Mutex<RecoveryStoreState>>,
    /// Executor that runs cleanup retries for retained groups.
    pub(in crate::v02::resource::recovery) executor: Arc<crate::v02::executor::CleanupExecutor>,
    /// Worker threads serving asynchronous cleanup retries.
    pub(in crate::v02::resource::recovery) async_workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// Shared shutdown state and notification for blocking waiters.
    pub(in crate::v02::resource::recovery) shutdown: Arc<(Mutex<RecoveryStoreShutdownState>, std::sync::Condvar)>,
    /// Notification source for asynchronous shutdown observers.
    pub(in crate::v02::resource::recovery) shutdown_notify: Arc<Notify>,
    /// Test-only gate for exercising publication before terminal callback
    /// installation.
    #[cfg(test)]
    pub(super) callback_install_gate: CallbackInstallGate,
}

impl RecoveryStore {
    /// Creates a recovery store with bounded capacity and cleanup workers.
    ///
    /// # Parameters
    ///
    /// - `options`: Group/resource capacities and cleanup executor settings.
    ///
    /// # Returns
    ///
    /// A store accepting transferred recovery groups.
    ///
    /// # Errors
    ///
    /// Returns an error when a capacity is zero or the cleanup executor or
    /// worker threads cannot be created.
    pub fn new(options: RecoveryStoreOptions) -> Result<Self, RecoveryStoreCreateError> {
        if options.group_capacity == 0 {
            return Err(RecoveryStoreCreateError::InvalidOptions {
                field: "group_capacity",
            });
        }
        if options.resource_capacity == 0 {
            return Err(RecoveryStoreCreateError::InvalidOptions {
                field: "resource_capacity",
            });
        }
        if options.cleanup_concurrency == 0 {
            return Err(RecoveryStoreCreateError::InvalidOptions {
                field: "cleanup_concurrency",
            });
        }
        if options.cleanup_executor.worker_count == 0 || options.cleanup_executor.queue_capacity == 0 {
            return Err(RecoveryStoreCreateError::InvalidOptions {
                field: "cleanup_executor",
            });
        }
        let executor = Arc::new(
            crate::v02::executor::CleanupExecutor::new(
                options.cleanup_executor,
                crate::v02::report::PortableExecutorOptions::default(),
            )
            .map_err(|_| RecoveryStoreCreateError::ExecutorUnavailable)?,
        );
        let workers = executor
            .start_async_workers(options.cleanup_concurrency, "qubit-operation-recovery")
            .map_err(|_| RecoveryStoreCreateError::ExecutorUnavailable)?;
        Ok(Self::from_parts(
            options.group_capacity,
            options.resource_capacity,
            executor,
            workers,
        ))
    }

    /// Creates a small recovery store for recovery unit tests with one worker.
    ///
    /// # Parameters
    ///
    /// - `group_capacity`: Maximum unresolved groups retained by the test
    ///   store.
    /// - `resource_capacity`: Maximum resources retained by the test store.
    ///
    /// # Returns
    ///
    /// `None` when either capacity is zero or the test executor cannot start.
    #[cfg(test)]
    pub(super) fn with_capacity(group_capacity: usize, resource_capacity: usize) -> Option<Self> {
        if group_capacity == 0 || resource_capacity == 0 {
            return None;
        }
        let executor = Arc::new(
            crate::v02::executor::CleanupExecutor::new(
                crate::v02::report::ThreadPoolExecutorOptions {
                    worker_count: 1,
                    queue_capacity: 8,
                },
                crate::v02::report::PortableExecutorOptions::default(),
            )
            .ok()?,
        );
        let workers = executor.start_async_workers(1, "qubit-operation-recovery-test").ok()?;
        Some(Self::from_parts(group_capacity, resource_capacity, executor, workers))
    }

    /// Initializes shared store state after its executor workers are ready.
    ///
    /// # Parameters
    ///
    /// - `group_capacity`: Maximum retained recovery groups.
    /// - `resource_capacity`: Maximum resources retained across those groups.
    /// - `executor`: Cleanup executor shared by retry workers.
    /// - `workers`: Worker threads that consume asynchronous retry tasks.
    ///
    /// # Returns
    ///
    /// A store with admission open and empty recovery state.
    fn from_parts(
        group_capacity: usize,
        resource_capacity: usize,
        executor: Arc<crate::v02::executor::CleanupExecutor>,
        workers: Vec<JoinHandle<()>>,
    ) -> Self {
        Self {
            id: RecoveryStoreId::new_unique(),
            group_capacity,
            resource_capacity,
            state: Arc::new(Mutex::new(RecoveryStoreState {
                accepting: true,
                groups: HashMap::with_capacity(group_capacity),
            })),
            executor,
            async_workers: Arc::new(Mutex::new(workers)),
            shutdown: Arc::new((
                Mutex::new(RecoveryStoreShutdownState {
                    started: false,
                    result: None,
                }),
                std::sync::Condvar::new(),
            )),
            shutdown_notify: Arc::new(Notify::new()),
            #[cfg(test)]
            callback_install_gate: Arc::new(Mutex::new(None)),
        }
    }

    /// Checks whether the store currently retains a recovery group.
    ///
    /// Resolved groups are removed before this check.
    ///
    /// # Parameters
    ///
    /// - `self`: Store whose retained groups are queried.
    /// - `id`: Recovery group identity to find.
    ///
    /// # Returns
    ///
    /// `true` when the group is still retained by this store.
    #[must_use = "use the result to determine whether the store retains this recovery group"]
    pub fn contains(&self, id: RecoveryId) -> bool {
        self.prune_resolved();
        lock(&self.state).groups.contains_key(&id)
    }

    /// Starts a cleanup retry for a group retained by this store.
    ///
    /// # Parameters
    ///
    /// - `self`: Store that owns the recovery group.
    /// - `id`: Recovery group identity to retry.
    ///
    /// # Returns
    ///
    /// An observer for the admitted cleanup attempt.
    ///
    /// # Errors
    ///
    /// Returns an error if the group is absent or resolved, another attempt is
    /// running, executor capacity is unavailable, or no retry action exists.
    pub fn try_retry(&self, id: RecoveryId) -> Result<RecoveryAttempt, crate::v02::report::RetryError> {
        let group = lock(&self.state)
            .groups
            .get(&id)
            .cloned()
            .ok_or(crate::v02::report::RetryError::AlreadyResolved)?;
        group.restore_from_store()?;
        group.try_retry()
    }

    /// Prevents this store from accepting additional recovery groups.
    ///
    /// Existing groups remain retained and can still be retried.
    ///
    /// # Parameters
    ///
    /// - `self`: Store whose admission is closed.
    ///
    /// # Returns
    ///
    /// `()` after admission has been disabled.
    pub fn close_admission(&self) {
        lock(&self.state).accepting = false;
    }

    /// Closes admission and returns a handle for draining and stopping workers.
    ///
    /// Shutdown starts when a wait method is called on the returned handle.
    ///
    /// # Parameters
    ///
    /// - `self`: Store to close and shut down.
    ///
    /// # Returns
    ///
    /// A shared shutdown handle; call one of its wait methods to complete
    /// shutdown.
    pub fn request_shutdown(&self) -> RecoveryStoreShutdownHandle {
        self.close_admission();
        RecoveryStoreShutdownHandle { store: self.clone() }
    }

    /// Returns counts and admission state for the retained recovery groups.
    ///
    /// Resolved groups are pruned before the snapshot is built.
    ///
    /// # Parameters
    ///
    /// - `self`: Store whose state is observed.
    ///
    /// # Returns
    ///
    /// A point-in-time snapshot of group/resource counts, active attempts, and
    /// admission state.
    #[must_use = "inspect the returned recovery store snapshot"]
    pub fn snapshot(&self) -> RecoveryStoreSnapshot {
        self.prune_resolved();
        let state = lock(&self.state);
        let (groups, resources) = store_usage(&state);
        RecoveryStoreSnapshot {
            accepting: state.accepting,
            groups,
            resources,
            running_attempts: state
                .groups
                .values()
                .filter(|group| group.snapshot().phase == RecoveryPhase::Running)
                .count(),
        }
    }

    /// Takes ownership of a complete recovery group when capacity permits.
    ///
    /// Group identity, manager ownership, and unique resource membership are
    /// checked before the group is inserted. The returned transfer receipt
    /// identifies this store as the receiving owner.
    ///
    /// # Parameters
    ///
    /// - `self`: Store that may accept the recovery group.
    /// - `group`: Recovery group whose ownership is being transferred.
    ///
    /// # Returns
    ///
    /// The transfer receipt on success. On failure, returns the reason and
    /// the unchanged group so its current owner can retain or retry it.
    ///
    /// # Errors
    ///
    /// Returns `Closed`, a capacity error, `InvalidGroup`, or
    /// `AlreadyTransferred` together with the rejected group.
    pub(in crate::v02::resource::recovery) fn accept(
        &self,
        group: Arc<RecoveryCore>,
    ) -> Result<RecoveryTransfer, (RecoveryStoreError, Arc<RecoveryCore>)> {
        self.prune_resolved();
        let mut state = lock(&self.state);
        if !state.accepting {
            return Err((RecoveryStoreError::Closed, group));
        }
        let (active_groups, active_resources) = store_usage(&state);
        if active_groups >= self.group_capacity {
            return Err((RecoveryStoreError::GroupCapacity, group));
        }
        let members = group.resource_count();
        if members == 0
            || group
                .resources
                .iter()
                .any(|resource| resource.manager() != group.resources[0].manager())
            || group
                .resources
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != members
        {
            return Err((RecoveryStoreError::InvalidGroup, group));
        }
        if active_resources.saturating_add(members) > self.resource_capacity {
            return Err((RecoveryStoreError::ResourceCapacity, group));
        }
        if state.groups.contains_key(&group.id()) || !group.mark_transferred() {
            return Err((RecoveryStoreError::AlreadyTransferred, group));
        }
        state.groups.reserve(1);
        *lock(&group.executor) = Some(Arc::clone(&self.executor));
        let weak_state = Arc::downgrade(&self.state);
        let executor = Arc::clone(&self.executor);
        *lock(&group.retry_route) = Some(Arc::new(move |id| {
            let state = weak_state
                .upgrade()
                .ok_or(crate::v02::report::RetryError::AlreadyResolved)?;
            let group = lock(&state)
                .groups
                .get(&id)
                .cloned()
                .ok_or(crate::v02::report::RetryError::AlreadyResolved)?;
            group.restore_from_store()?;
            *lock(&group.executor) = Some(Arc::clone(&executor));
            group.try_retry()
        }));
        state.groups.insert(group.id(), Arc::clone(&group));
        let nonce = next_transfer_nonce();
        let transfer = RecoveryTransfer {
            recovery: group.id(),
            receiver: self.id,
            receipt: RecoveryTransferReceipt::new(group.id(), self.id, nonce),
        };
        drop(state);
        #[cfg(test)]
        if let Some(gate) = lock(&self.callback_install_gate).clone() {
            gate.0.wait();
            gate.1.wait();
        }
        group.notify_terminal(false);
        let notify = Arc::downgrade(&self.shutdown_notify);
        group.set_terminal_callback(Arc::new(move |_| {
            if let Some(notify) = notify.upgrade() {
                notify.notify_waiters();
            }
        }));
        // A retry may resolve the group after publication but before callback
        // installation. Recheck after installing the callback to close that
        // missed-notification window.
        if group.snapshot().phase == RecoveryPhase::Resolved {
            self.shutdown_notify.notify_waiters();
        }
        Ok(transfer)
    }

    /// Removes resolved groups so they no longer consume store capacity.
    ///
    /// Dropping removed groups happens after releasing the store lock because
    /// their final payload/action destructors may run arbitrary code.
    ///
    /// # Parameters
    ///
    /// - `self`: Store whose resolved entries are removed.
    fn prune_resolved(&self) {
        let removed = {
            let mut state = lock(&self.state);
            let resolved = state
                .groups
                .iter()
                .filter_map(|(id, group)| (group.snapshot().phase == RecoveryPhase::Resolved).then_some(*id))
                .collect::<Vec<_>>();
            resolved
                .into_iter()
                .filter_map(|id| state.groups.remove(&id))
                .collect::<Vec<_>>()
        };
        // The last group reference may run arbitrary payload/action destructors.
        drop(removed);
    }
}

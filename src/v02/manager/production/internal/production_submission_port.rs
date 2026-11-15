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
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;

use super::super::lock;
use super::super::map_executor_admission;
use super::super::map_executor_create_error;
use super::production_lease::ProductionLease;
use super::production_permit::ProductionPermit;
use crate::v02::executor::CleanupExecutor;
use crate::v02::executor::PortableExecutor;
use crate::v02::executor::ThreadPoolExecutor;
use crate::v02::manager::capacity::CapacityPermit;
use crate::v02::manager::capacity::ManagerCapacityPools;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::manager::core::ExecutionKind;
use crate::v02::manager::core::ManagerPhase;
use crate::v02::manager::core::SubmissionPort;
use crate::v02::manager::core::SubmissionReservation;
use crate::v02::report::AdmissionError;
use crate::v02::report::ManagerCreateError;
use crate::v02::report::ManagerId;
use crate::v02::report::ManagerOptions;
use crate::v02::report::OperationId;
use crate::v02::report::OperationOptions;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ScopeLimits;
use crate::v02::resource::ResourceScope;
use crate::v02::resource::cleanup::CleanupDispatcher;
use crate::v02::resource::cleanup::CleanupDispatcherError;
use crate::v02::resource::cleanup::CleanupPump;

/// Owns manager executors, admission pools, and their shutdown coordination.
#[must_use]
pub(in crate::v02::manager::production) struct ProductionSubmissionPort {
    /// Identity shared by resources created through this manager.
    manager_id: ManagerId,
    /// Manager-wide capacity pools and their wake coordinator.
    capacities: ManagerCapacityPools,
    /// Blocking executor for synchronous operation bodies.
    sync: Arc<ThreadPoolExecutor>,
    /// Future executor used by asynchronous operation bodies.
    asynchronous: PortableExecutor,
    /// Worker handles joined after async admission closes.
    async_workers: Mutex<Vec<JoinHandle<()>>>,
    /// Executor that performs resource cleanup actions.
    cleanup: Arc<CleanupExecutor>,
    /// Pump that transfers cleanup tasks when cleanup capacity becomes
    /// available.
    cleanup_pump: Arc<CleanupPump>,
    /// Cleanup worker handles joined during shutdown.
    cleanup_workers: Mutex<Vec<JoinHandle<()>>>,
    /// Ensures shutdown joins executors at most once.
    shutdown_finished: AtomicBool,
    /// Encoded manager phase observed by the executor assembly.
    phase: AtomicU8,
    /// Number of submitted jobs not yet started by a worker.
    queued_operations: Arc<AtomicUsize>,
}

impl ProductionSubmissionPort {
    /// Starts executors and cleanup infrastructure, rolling prior parts back on
    /// failure.
    ///
    /// # Parameters
    ///
    /// - `options`: Configures manager capacities and its executors.
    ///
    /// # Returns
    ///
    /// The initialized production submission port.
    ///
    /// # Errors
    ///
    /// Returns an error if options are invalid or any executor or worker fails
    /// to start.
    pub(in crate::v02::manager::production) fn new(options: &ManagerOptions) -> Result<Self, ManagerCreateError> {
        let capacities = ManagerCapacityPools::new(options.limits);
        let cleanup_pump = CleanupPump::new().map_err(|source| ManagerCreateError::ThreadSpawn {
            source: Arc::new(source),
        })?;
        let sync = match ThreadPoolExecutor::new(options.sync_executor) {
            Ok(executor) => Arc::new(executor),
            Err(error) => {
                cleanup_pump.shutdown();
                return Err(map_executor_create_error(error));
            }
        };
        let asynchronous = match PortableExecutor::new(options.async_executor) {
            Ok(executor) => executor,
            Err(error) => {
                let _ = sync.shutdown().wait();
                cleanup_pump.shutdown();
                return Err(map_executor_create_error(error));
            }
        };
        let async_workers =
            match asynchronous.start_workers(options.async_executor.worker_count, "qubit-operation-async") {
                Ok(workers) => workers,
                Err(error) => {
                    asynchronous.close_admission();
                    let _ = sync.shutdown().wait();
                    cleanup_pump.shutdown();
                    return Err(map_executor_create_error(error));
                }
            };
        let cleanup = match CleanupExecutor::new(options.cleanup_executor, options.async_executor) {
            Ok(executor) => Arc::new(executor),
            Err(error) => {
                asynchronous.close_admission();
                for worker in async_workers {
                    let _ = worker.join();
                }
                let _ = sync.shutdown().wait();
                cleanup_pump.shutdown();
                return Err(map_executor_create_error(error));
            }
        };
        let weak_pump = Arc::downgrade(&cleanup_pump);
        cleanup.set_capacity_callback(Arc::new(move || {
            if let Some(pump) = weak_pump.upgrade() {
                pump.kick();
            }
        }));
        let cleanup_workers =
            match cleanup.start_async_workers(options.async_executor.worker_count, "qubit-operation-cleanup-async") {
                Ok(workers) => workers,
                Err(error) => {
                    let _ = cleanup.shutdown().wait();
                    asynchronous.close_admission();
                    for worker in async_workers {
                        let _ = worker.join();
                    }
                    let _ = sync.shutdown().wait();
                    cleanup_pump.shutdown();
                    return Err(map_executor_create_error(error));
                }
            };
        Ok(Self {
            manager_id: ManagerId::new_unique(),
            capacities,
            sync,
            asynchronous,
            async_workers: Mutex::new(async_workers),
            cleanup,
            cleanup_pump,
            cleanup_workers: Mutex::new(cleanup_workers),
            shutdown_finished: AtomicBool::new(false),
            phase: AtomicU8::new(0),
            queued_operations: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Reserves the executor matching the operation kind and retains its queue
    /// slot.
    ///
    /// # Parameters
    ///
    /// - `self`: Port that owns the selected executor and queue capacity.
    /// - `kind`: Selects the synchronous or asynchronous executor.
    /// - `queue`: Retains the queue-capacity permit for the accepted job.
    ///
    /// # Returns
    ///
    /// The executor permit that retains its queue slot until the job starts.
    ///
    /// # Errors
    ///
    /// Returns an admission error if the selected executor cannot reserve a
    /// job slot.
    fn execution_permit(&self, kind: ExecutionKind, queue: CapacityPermit) -> Result<ProductionPermit, AdmissionError> {
        match kind {
            ExecutionKind::Sync => self
                .sync
                .reserve()
                .map(|permit| ProductionPermit::Sync(permit, Arc::clone(&self.queued_operations), queue))
                .map_err(map_executor_admission),
            ExecutionKind::Async => self
                .asynchronous
                .reserve()
                .map(|permit| ProductionPermit::Async(permit, Arc::clone(&self.queued_operations), queue))
                .map_err(map_executor_admission),
        }
    }

    /// Builds a root or inherited capacity path for one operation identity.
    ///
    /// # Parameters
    ///
    /// - `self`: Port providing manager-wide capacity pools.
    /// - `scope_id`: Identifies the operation's capacity pools.
    /// - `parent`: Supplies the parent scope when creating a child path.
    /// - `limits`: Sets requested capacities, clamped by inherited limits.
    ///
    /// # Returns
    ///
    /// The operation's hierarchical capacity path.
    fn scope_capacity(
        &self,
        scope_id: OperationId,
        parent: Option<&crate::v02::scope::ScopeCore>,
        limits: ScopeLimits,
    ) -> Arc<ScopeCapacity> {
        Arc::new(match parent {
            Some(parent) => parent.capacity().child_for_operation(scope_id, limits),
            None => ScopeCapacity::root_for_operation(&self.capacities, scope_id, limits),
        })
    }
}

impl Drop for ProductionSubmissionPort {
    /// Closes admissions without waiting for workers from a public-handle drop.
    ///
    /// # Parameters
    ///
    /// - `self`: Port whose executors are being closed.
    fn drop(&mut self) {
        self.asynchronous.close_admission();
        self.cleanup.close_admission();
        // ThreadPoolExecutor Drop closes its admission and detaches any join
        // handles; no public-handle Drop waits for workers.
    }
}

impl SubmissionPort for ProductionSubmissionPort {
    /// Reserves lifecycle, executor, scope-capacity, and cleanup resources for
    /// an operation.
    ///
    /// # Parameters
    ///
    /// - `self`: Port reserving resources for the operation.
    /// - `scope_id`: Identifies the operation scope.
    /// - `execution`: Selects its synchronous or asynchronous executor.
    /// - `parent`: Supplies the parent scope for a child operation.
    /// - `options`: Provides the requested scope limits.
    ///
    /// # Returns
    ///
    /// A reservation containing the executor, lifecycle, capacity, and
    /// resource-scope permits.
    ///
    /// # Errors
    ///
    /// Returns an admission error if capacity, executor, or cleanup
    /// infrastructure cannot be reserved.
    fn reserve(
        &self,
        scope_id: OperationId,
        execution: ExecutionKind,
        parent: Option<&crate::v02::scope::ScopeCore>,
        options: &OperationOptions,
    ) -> Result<SubmissionReservation, AdmissionError> {
        let capacity = self.scope_capacity(scope_id, parent, options.limits);
        let (active_root, child_path, outcome) = if let Some(parent) = parent {
            let (path, outcome) = self
                .capacities
                .child_lifecycle_permits(parent.capacity())
                .map_err(|_| AdmissionError::Busy)?;
            (None, Some(path), outcome)
        } else {
            let (active, outcome) = self
                .capacities
                .root_lifecycle_permits()
                .map_err(|_| AdmissionError::Busy)?;
            (Some(active), None, outcome)
        };
        let queue = match self.capacities.queue_permit() {
            Ok(queue) => queue,
            Err(_) => {
                drop(active_root);
                drop(child_path);
                drop(outcome);
                return Err(AdmissionError::Busy);
            }
        };
        let execution = match self.execution_permit(execution, queue) {
            Ok(permit) => permit,
            Err(error) => {
                drop(active_root);
                drop(child_path);
                drop(outcome);
                return Err(error);
            }
        };
        let dispatcher = Arc::new(
            match CleanupDispatcher::new_with_pump(
                capacity.effective_limits().resources,
                Arc::clone(&self.cleanup),
                Arc::clone(&self.cleanup_pump),
            ) {
                Ok(dispatcher) => dispatcher,
                Err(CleanupDispatcherError::ZeroCapacity) => {
                    drop(execution);
                    drop(active_root);
                    drop(child_path);
                    drop(outcome);
                    return Err(AdmissionError::CapacityExceeded);
                }
            },
        );
        let resource_scope = ResourceScope::new(
            self.manager_id,
            ResourceScopeId::new_unique(),
            Arc::clone(&capacity),
            dispatcher,
        );
        Ok(SubmissionReservation {
            execution: Box::new(execution),
            lifecycle: Arc::new(ProductionLease::new(active_root, child_path, outcome)),
            capacity,
            resource_scope,
        })
    }

    /// Waits asynchronously until the root or child admission pools can be
    /// acquired.
    ///
    /// # Parameters
    ///
    /// - `self`: Port whose admission pools are awaited.
    /// - `parent`: Supplies the parent scope when retrying a child operation.
    /// - `_options`: Carries the original operation options for retry parity.
    ///
    /// # Returns
    ///
    /// A future that resolves when capacity is available or reports an
    /// admission error.
    ///
    /// # Errors
    ///
    /// The returned future reports an admission error if capacity cannot be
    /// reserved.
    fn wait_for_capacity<'a>(
        &'a self,
        parent: Option<Arc<crate::v02::scope::ScopeCore>>,
        _options: OperationOptions,
    ) -> Pin<Box<dyn Future<Output = Result<(), AdmissionError>> + Send + 'a>> {
        let request = if let Some(parent) = parent {
            self.capacities.child_admission_request(parent.capacity())
        } else {
            self.capacities.root_admission_request()
        };
        match request {
            Ok(request) => Box::pin(async move { request.await.map(|_| ()).map_err(|_| AdmissionError::Busy) }),
            Err(_) => Box::pin(std::future::ready(Err(AdmissionError::Busy))),
        }
    }

    /// Publishes a manager phase change to capacity waiters.
    ///
    /// # Parameters
    ///
    /// - `self`: Port whose shared manager phase changed.
    /// - `phase`: The new shared manager phase.
    fn phase_changed(&self, phase: ManagerPhase) {
        self.phase.store(phase as u8, Ordering::Release);
        self.capacities.notify_waiters();
    }

    /// Closes and joins sync, async, and cleanup workers once manager state
    /// drains.
    ///
    /// # Parameters
    ///
    /// - `self`: Port whose executors and worker threads are shut down.
    fn finish_shutdown(&self) {
        if self.shutdown_finished.swap(true, Ordering::AcqRel) {
            return;
        }
        let _ = self.sync.shutdown().wait();
        self.asynchronous.close_admission();
        for worker in std::mem::take(&mut *lock(&self.async_workers)) {
            let _ = worker.join();
        }
        let _ = self.cleanup.shutdown().wait();
        for worker in std::mem::take(&mut *lock(&self.cleanup_workers)) {
            let _ = worker.join();
        }
        self.cleanup_pump.shutdown();
    }

    /// Returns the number of admitted executor jobs that have not started.
    ///
    /// # Parameters
    ///
    /// - `self`: Port whose queued operation count is read.
    ///
    /// # Returns
    ///
    /// The number of admitted jobs that have not started.
    #[inline]
    fn queued_operation_count(&self) -> usize {
        self.queued_operations.load(Ordering::Acquire)
    }

    /// Creates a manager-lifetime resource scope with its own cleanup
    /// dispatcher.
    ///
    /// # Parameters
    ///
    /// - `self`: Port creating the manager-lifetime resource scope.
    /// - `capacity`: The maximum number of resources managed by the scope.
    ///
    /// # Returns
    ///
    /// The manager-lifetime resource scope with its cleanup dispatcher.
    ///
    /// # Errors
    ///
    /// Returns a resource-scope error if capacity is zero or its cleanup
    /// dispatcher cannot be created.
    fn service_resource_scope(&self, capacity: usize) -> Result<ResourceScope, crate::v02::report::ResourceScopeError> {
        if capacity == 0 {
            return Err(crate::v02::report::ResourceScopeError::InvalidOptions);
        }
        let id = ResourceScopeId::new_unique();
        let limits = ScopeLimits {
            child_operations: 1,
            resources: capacity,
            resource_scopes: 1,
            effect_records: 1,
        };
        let resource_capacity = Arc::new(ScopeCapacity::root_with_identity(&self.capacities, id.raw(), limits));
        let dispatcher = Arc::new(
            CleanupDispatcher::new_with_pump(capacity, Arc::clone(&self.cleanup), Arc::clone(&self.cleanup_pump))
                .map_err(|_| crate::v02::report::ResourceScopeError::CapacityExceeded)?,
        );
        Ok(ResourceScope::new(self.manager_id, id, resource_capacity, dispatcher))
    }

    /// Creates an operation resource scope using its inherited resource
    /// capacity.
    ///
    /// # Parameters
    ///
    /// - `self`: Port creating the operation-owned resource scope.
    /// - `id`: Identifies the resource scope.
    /// - `capacity`: Supplies the inherited resource capacity path.
    ///
    /// # Returns
    ///
    /// The operation-owned resource scope with its cleanup dispatcher.
    ///
    /// # Errors
    ///
    /// Returns a resource-scope error if its cleanup dispatcher cannot be
    /// created.
    fn operation_resource_scope(
        &self,
        id: ResourceScopeId,
        capacity: Arc<ScopeCapacity>,
    ) -> Result<ResourceScope, crate::v02::report::ResourceScopeError> {
        let dispatcher = Arc::new(
            CleanupDispatcher::new_with_pump(
                capacity.effective_limits().resources,
                Arc::clone(&self.cleanup),
                Arc::clone(&self.cleanup_pump),
            )
            .map_err(|_| crate::v02::report::ResourceScopeError::CapacityExceeded)?,
        );
        Ok(ResourceScope::new(self.manager_id, id, capacity, dispatcher))
    }
}

//! Production assembly for manager capacity, scopes, and executors.

use super::capacity::{CapacityPermit, ManagerCapacityPools, PermitBundle, ScopeCapacity};
use super::core::{
    AsyncOperationManager, ExecutionKind, ManagerCore, ManagerPhase, OperationJob,
    OperationManager, SubmissionLease, SubmissionPermit, SubmissionPort, SubmissionReservation,
};
use crate::v02::context::OperationContext;
use crate::v02::executor::{
    CleanupExecutor, ExecutorCreateError, ExecutorError, PortableExecutor, ThreadPoolExecutor,
};
use crate::v02::report::{
    AdmissionError, ManagerCreateError, ManagerId, ManagerOptions, OperationId, OperationOptions,
    ResourceScopeId, ScopeLimits,
};
use crate::v02::resource::ResourceScope;
use crate::v02::resource::cleanup::{CleanupDispatcher, CleanupDispatcherError, CleanupPump};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

pub(crate) fn new_sync(options: ManagerOptions) -> Result<OperationManager, ManagerCreateError> {
    validate_options(&options)?;
    let root_context = OperationContext::new(crate::v02::report::ContextOptions {
        clock: options.clock.clone(),
        deadline: None,
        budget: None,
    })
    .map_err(|_| ManagerCreateError::InvalidOptions { field: "clock" })?;
    let port = ProductionSubmissionPort::new(&options)?;
    let core = ManagerCore::new(Arc::new(port), root_context, options.scope_limits);
    Ok(core.root_view())
}

pub(crate) fn new_async(
    options: ManagerOptions,
) -> Result<AsyncOperationManager, ManagerCreateError> {
    new_sync(options).map(|manager| manager.as_async())
}

fn validate_options(options: &ManagerOptions) -> Result<(), ManagerCreateError> {
    let limits = options.limits;
    for (field, value) in [
        ("limits.active_operations", limits.active_operations),
        ("limits.queued_operations", limits.queued_operations),
        ("limits.admission_waiters", limits.admission_waiters),
        ("limits.unconsumed_outcomes", limits.unconsumed_outcomes),
        ("limits.cleanup_concurrency", limits.cleanup_concurrency),
        ("limits.diagnostic_events", limits.diagnostic_events),
        (
            "scope_limits.child_operations",
            options.scope_limits.child_operations,
        ),
        ("scope_limits.resources", options.scope_limits.resources),
        (
            "scope_limits.effect_records",
            options.scope_limits.effect_records,
        ),
        (
            "async_executor.worker_count",
            options.async_executor.worker_count,
        ),
        (
            "async_executor.task_capacity",
            options.async_executor.task_capacity,
        ),
        (
            "async_executor.polls_per_turn",
            options.async_executor.polls_per_turn,
        ),
        (
            "sync_executor.worker_count",
            options.sync_executor.worker_count,
        ),
        (
            "sync_executor.queue_capacity",
            options.sync_executor.queue_capacity,
        ),
        (
            "cleanup_executor.worker_count",
            options.cleanup_executor.worker_count,
        ),
        (
            "cleanup_executor.queue_capacity",
            options.cleanup_executor.queue_capacity,
        ),
    ] {
        if value == 0 {
            return Err(ManagerCreateError::InvalidOptions { field });
        }
    }
    Ok(())
}

struct ProductionSubmissionPort {
    manager_id: ManagerId,
    capacities: ManagerCapacityPools,
    sync: Arc<ThreadPoolExecutor>,
    asynchronous: PortableExecutor,
    async_workers: Mutex<Vec<JoinHandle<()>>>,
    cleanup: Arc<CleanupExecutor>,
    cleanup_pump: Arc<CleanupPump>,
    cleanup_workers: Mutex<Vec<JoinHandle<()>>>,
    shutdown_finished: AtomicBool,
    phase: AtomicU8,
}

impl ProductionSubmissionPort {
    fn new(options: &ManagerOptions) -> Result<Self, ManagerCreateError> {
        let capacities = ManagerCapacityPools::new(options.limits);
        let cleanup_pump =
            CleanupPump::new().map_err(|source| ManagerCreateError::ThreadSpawn {
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
        let async_workers = match asynchronous
            .start_workers(options.async_executor.worker_count, "qubit-operation-async")
        {
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
        let cleanup_workers = match cleanup.start_async_workers(
            options.async_executor.worker_count,
            "qubit-operation-cleanup-async",
        ) {
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
        })
    }

    fn execution_permit(&self, kind: ExecutionKind) -> Result<ProductionPermit, AdmissionError> {
        match kind {
            ExecutionKind::Sync => self
                .sync
                .reserve()
                .map(ProductionPermit::Sync)
                .map_err(map_executor_admission),
            ExecutionKind::Async => self
                .asynchronous
                .reserve()
                .map(ProductionPermit::Async)
                .map_err(map_executor_admission),
        }
    }

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
    fn drop(&mut self) {
        self.asynchronous.close_admission();
        self.cleanup.close_admission();
        // ThreadPoolExecutor Drop closes its admission and detaches any join
        // handles; no public-handle Drop waits for workers.
    }
}

impl SubmissionPort for ProductionSubmissionPort {
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
        let execution = match self.execution_permit(execution) {
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
            Ok(request) => {
                Box::pin(async move { request.await.map(|_| ()).map_err(|_| AdmissionError::Busy) })
            }
            Err(_) => Box::pin(std::future::ready(Err(AdmissionError::Busy))),
        }
    }

    fn phase_changed(&self, phase: ManagerPhase) {
        self.phase.store(phase as u8, Ordering::Release);
        self.capacities.notify_waiters();
    }

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

    fn service_resource_scope(
        &self,
        capacity: usize,
    ) -> Result<ResourceScope, crate::v02::report::ResourceScopeError> {
        if capacity == 0 {
            return Err(crate::v02::report::ResourceScopeError::InvalidOptions);
        }
        let id = ResourceScopeId::new_unique();
        let limits = ScopeLimits {
            child_operations: 1,
            resources: capacity,
            effect_records: 1,
        };
        let resource_capacity = Arc::new(ScopeCapacity::root_with_identity(
            &self.capacities,
            id.raw(),
            limits,
        ));
        let dispatcher = Arc::new(
            CleanupDispatcher::new_with_pump(
                capacity,
                Arc::clone(&self.cleanup),
                Arc::clone(&self.cleanup_pump),
            )
            .map_err(|_| crate::v02::report::ResourceScopeError::CapacityExceeded)?,
        );
        Ok(ResourceScope::new(
            self.manager_id,
            id,
            resource_capacity,
            dispatcher,
        ))
    }
}

enum ProductionPermit {
    Sync(crate::v02::executor::ThreadPoolPermit),
    Async(crate::v02::executor::PortablePermit),
}

impl SubmissionPermit for ProductionPermit {
    fn dispatch(self: Box<Self>, job: OperationJob) {
        match (*self, job) {
            (Self::Sync(permit), OperationJob::Sync(job)) => permit.submit(job),
            (Self::Async(permit), OperationJob::Async(future)) => permit.spawn(future),
            _ => unreachable!("ManagerCore dispatch kind matches its reservation"),
        }
    }
}

struct ProductionLease {
    state: Mutex<LeaseState>,
}

struct LeaseState {
    active_root: Option<CapacityPermit>,
    child_path: Option<PermitBundle>,
    outcome: Option<CapacityPermit>,
    settled: bool,
    delivered: bool,
}

impl ProductionLease {
    fn new(
        active_root: Option<CapacityPermit>,
        child_path: Option<PermitBundle>,
        outcome: CapacityPermit,
    ) -> Self {
        Self {
            state: Mutex::new(LeaseState {
                active_root,
                child_path,
                outcome: Some(outcome),
                settled: false,
                delivered: false,
            }),
        }
    }
}

impl SubmissionLease for ProductionLease {
    fn execution_finished(&self) {}

    fn settlement_finished(&self) {
        let (active, child) = {
            let mut state = lock(&self.state);
            state.settled = true;
            let active = state.active_root.take();
            let child = state.delivered.then(|| state.child_path.take()).flatten();
            (active, child)
        };
        drop(active);
        drop(child);
    }

    fn outcome_delivered(&self, _delivery: crate::v02::operation::OutcomeDelivery) {
        let (outcome, child) = {
            let mut state = lock(&self.state);
            state.delivered = true;
            let outcome = state.outcome.take();
            let child = state.settled.then(|| state.child_path.take()).flatten();
            (outcome, child)
        };
        drop(outcome);
        drop(child);
    }
}

fn map_executor_admission(error: ExecutorError) -> AdmissionError {
    match error {
        ExecutorError::Full => AdmissionError::Busy,
        ExecutorError::Closed => AdmissionError::ExecutorUnavailable,
    }
}

fn map_executor_create_error(error: ExecutorCreateError) -> ManagerCreateError {
    match error {
        ExecutorCreateError::InvalidOptions => ManagerCreateError::InvalidOptions {
            field: "executor options",
        },
        ExecutorCreateError::ThreadSpawn => ManagerCreateError::ThreadSpawn {
            source: Arc::new(std::io::Error::other("worker thread spawn failed")),
        },
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use crate::v02::manager::core::{AsyncOperationManager, OperationManager};
    use crate::v02::report::{
        ManagerOptions, OperationOptions, OperationOutcome, ShutdownMode, ShutdownWait,
    };
    use std::time::Duration;

    #[test]
    fn real_sync_and_async_managers_run_typed_operations_and_shutdown() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let mut sync = manager
            .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(17usize))
            .unwrap();
        assert!(matches!(
            sync.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(17)
        ));
        assert!(sync.wait_settlement().unwrap().all_released());
        assert!(matches!(
            manager
                .request_shutdown(ShutdownMode::Drain)
                .blocking()
                .wait_for(Duration::from_secs(3)),
            ShutdownWait::Completed(_)
        ));

        let manager = AsyncOperationManager::new(ManagerOptions::default()).unwrap();
        let mut asynchronous = manager
            .try_submit(OperationOptions::default(), |_| async {
                Ok::<_, ()>(23usize)
            })
            .unwrap()
            .into_sync();
        assert!(matches!(
            asynchronous.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(23)
        ));
        assert!(asynchronous.wait_settlement().unwrap().all_released());
        assert!(matches!(
            manager
                .as_sync()
                .request_shutdown(ShutdownMode::Drain)
                .blocking()
                .wait_for(Duration::from_secs(3)),
            ShutdownWait::Completed(_)
        ));
    }
}

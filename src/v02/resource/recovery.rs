//! Strongly retained recovery payloads and single-flight retry state.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::v02::report::{
    CleanupFailure, RecoveryId, RecoveryPhase, RecoverySnapshot, RecoveryStoreCreateError,
    RecoveryStoreId, RecoveryStoreOptions, RecoveryStoreSnapshot, RecoveryTransfer,
    RecoveryTransferError, RecoveryTransferReceipt, ResourceId,
};
use std::fmt;

static NEXT_RECOVERY_TRANSFER: AtomicU64 = AtomicU64::new(1);

pub(crate) type RecoveryPayload = Arc<dyn Any + Send + Sync + 'static>;
pub(crate) type RecoveryFuture =
    Pin<Box<dyn Future<Output = Result<(), CleanupFailure>> + Send + 'static>>;

/// Retry strategy operates through a shared retained owner capsule. The
/// capsule remains in `RecoveryCore` across failures and future cancellation.
pub(crate) trait RecoveryAction: Send + Sync + 'static {
    fn retry(&self, payload: RecoveryPayload) -> RecoveryFuture;
}

struct AsyncRecoveryOwner<T: Send + Sync + 'static> {
    resource_id: ResourceId,
    resource: Mutex<Option<T>>,
    registration: Mutex<Option<crate::v02::manager::capacity::ResourceRegistration>>,
    policy: Arc<dyn super::cleanup::AsyncCleanup<T>>,
}

struct AsyncRecoveryAction<T: Send + Sync + 'static>(Arc<AsyncRecoveryOwner<T>>);

impl<T: Send + Sync + 'static> RecoveryAction for AsyncRecoveryAction<T> {
    fn retry(&self, _payload: RecoveryPayload) -> RecoveryFuture {
        let owner = Arc::clone(&self.0);
        Box::pin(async move {
            let resource = lock(&owner.resource).take().ok_or_else(|| CleanupFailure {
                resource_id: owner.resource_id,
                stage: crate::v02::report::CleanupStage::AsyncRelease,
                summary: crate::v02::report::DiagnosticSummary::new(
                    "recovery owner is unavailable",
                ),
                source: None,
            })?;
            let context = crate::v02::context::CleanupContext::new(Default::default())
                .expect("empty cleanup context is valid");
            match Arc::clone(&owner.policy).cleanup(resource, context).await {
                super::cleanup::CleanupOutcome::Released
                | super::cleanup::CleanupOutcome::Transferred(_) => {
                    drop(lock(&owner.registration).take());
                    Ok(())
                }
                super::cleanup::CleanupOutcome::Failed { failure, resource } => {
                    *lock(&owner.resource) = Some(resource);
                    Err(failure)
                }
            }
        })
    }
}

pub(crate) fn async_owner<T: Send + Sync + 'static>(
    resource_id: ResourceId,
    resource: T,
    policy: Arc<dyn super::cleanup::AsyncCleanup<T>>,
    registration: crate::v02::manager::capacity::ResourceRegistration,
) -> (RecoveryPayload, Arc<dyn RecoveryAction>) {
    let owner = Arc::new(AsyncRecoveryOwner {
        resource_id,
        resource: Mutex::new(Some(resource)),
        registration: Mutex::new(Some(registration)),
        policy,
    });
    (
        Arc::clone(&owner) as RecoveryPayload,
        Arc::new(AsyncRecoveryAction(owner)),
    )
}

struct State {
    phase: RecoveryPhase,
    attempts: usize,
    generation: u64,
    payload: Option<RecoveryPayload>,
    action: Option<Arc<dyn RecoveryAction>>,
    failures: Vec<CleanupFailure>,
}

/// One dependency group with an owner capsule that outlives every public
/// handle and every retry attempt.
pub(crate) struct RecoveryCore {
    id: RecoveryId,
    resources: Arc<[ResourceId]>,
    state: Mutex<State>,
    terminal: Mutex<Option<Arc<dyn Fn(bool) + Send + Sync + 'static>>>,
    executor: Mutex<Option<Arc<crate::v02::executor::CleanupExecutor>>>,
    retry_route: Mutex<
        Option<
            Arc<
                dyn Fn(RecoveryId) -> Result<RecoveryAttempt, crate::v02::report::RetryError>
                    + Send
                    + Sync,
            >,
        >,
    >,
}

impl Drop for RecoveryCore {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.phase != RecoveryPhase::Resolved {
            if let Some(payload) = state.payload.take() {
                std::mem::forget(payload);
            }
            if let Some(action) = state.action.take() {
                std::mem::forget(action);
            }
        }
    }
}

impl RecoveryCore {
    pub(crate) fn new(
        resources: Vec<ResourceId>,
        payload: RecoveryPayload,
        action: Arc<dyn RecoveryAction>,
    ) -> Self {
        Self {
            id: RecoveryId::new_unique(),
            resources: resources.into(),
            state: Mutex::new(State {
                phase: RecoveryPhase::Pending,
                attempts: 0,
                generation: 0,
                payload: Some(payload),
                action: Some(action),
                failures: Vec::new(),
            }),
            terminal: Mutex::new(None),
            executor: Mutex::new(None),
            retry_route: Mutex::new(None),
        }
    }

    pub(crate) fn new_unconfirmed(resources: Vec<ResourceId>, payload: RecoveryPayload) -> Self {
        Self {
            id: RecoveryId::new_unique(),
            resources: resources.into(),
            state: Mutex::new(State {
                phase: RecoveryPhase::Unconfirmed,
                attempts: 0,
                generation: 0,
                payload: Some(payload),
                action: None,
                failures: Vec::new(),
            }),
            terminal: Mutex::new(None),
            executor: Mutex::new(None),
            retry_route: Mutex::new(None),
        }
    }

    pub(crate) fn snapshot(&self) -> RecoverySnapshot {
        let state = lock(&self.state);
        RecoverySnapshot {
            id: self.id,
            phase: state.phase,
            resources: Arc::clone(&self.resources),
            attempts: state.attempts,
            last_failures: state.failures.clone().into(),
        }
    }

    pub(crate) fn try_retry(
        self: &Arc<Self>,
    ) -> Result<RecoveryAttempt, crate::v02::report::RetryError> {
        let route = {
            let state = lock(&self.state);
            if state.phase == RecoveryPhase::Transferred {
                lock(&self.retry_route).clone()
            } else {
                None
            }
        };
        if let Some(route) = route {
            return route(self.id);
        }
        let executor = lock(&self.executor).clone();
        let permit = match executor.as_ref() {
            Some(executor) => Some(
                executor
                    .reserve_async()
                    .map_err(|_| crate::v02::report::RetryError::NoCapacity)?,
            ),
            None => None,
        };
        let (generation, payload, action) = {
            let mut state = lock(&self.state);
            match state.phase {
                RecoveryPhase::Running => {
                    return Err(crate::v02::report::RetryError::AlreadyRunning);
                }
                RecoveryPhase::Resolved | RecoveryPhase::Transferred => {
                    return Err(crate::v02::report::RetryError::AlreadyResolved);
                }
                RecoveryPhase::Pending | RecoveryPhase::Unconfirmed => {}
            }
            let payload = state
                .payload
                .as_ref()
                .cloned()
                .ok_or(crate::v02::report::RetryError::AlreadyResolved)?;
            let action = state
                .action
                .as_ref()
                .cloned()
                .ok_or(crate::v02::report::RetryError::ExecutorUnavailable)?;
            state.phase = RecoveryPhase::Running;
            state.attempts += 1;
            state.generation = state.generation.wrapping_add(1);
            (state.generation, payload, action)
        };
        Ok(RecoveryAttempt {
            core: Arc::clone(self),
            run: Arc::new(Mutex::new(Some(RecoveryRun {
                core: Arc::clone(self),
                generation,
                payload: Some(payload),
                action,
                completed: false,
                permit,
            }))),
            observation: Arc::new(AttemptObservation::default()),
        })
    }

    pub(crate) fn mark_transferred(&self) -> bool {
        let mut state = lock(&self.state);
        if state.phase != RecoveryPhase::Pending && state.phase != RecoveryPhase::Unconfirmed {
            return false;
        }
        state.phase = RecoveryPhase::Transferred;
        drop(state);
        true
    }

    pub(crate) fn set_terminal_callback(
        &self,
        callback: Arc<dyn Fn(bool) + Send + Sync + 'static>,
    ) {
        *lock(&self.terminal) = Some(callback);
    }

    fn notify_terminal(&self, present: bool) {
        let callback = lock(&self.terminal).clone();
        if let Some(callback) = callback {
            callback(present);
        }
    }

    fn restore_from_store(&self) -> Result<(), crate::v02::report::RetryError> {
        let mut state = lock(&self.state);
        match state.phase {
            RecoveryPhase::Transferred => {
                state.phase = RecoveryPhase::Pending;
                Ok(())
            }
            RecoveryPhase::Pending | RecoveryPhase::Unconfirmed => Ok(()),
            RecoveryPhase::Running => Err(crate::v02::report::RetryError::AlreadyRunning),
            RecoveryPhase::Resolved => Err(crate::v02::report::RetryError::AlreadyResolved),
        }
    }

    pub(crate) fn id(&self) -> RecoveryId {
        self.id
    }

    fn resource_count(&self) -> usize {
        self.resources.len()
    }
}

/// Shared observation and retry capability for one retained recovery group.
#[derive(Clone)]
pub struct RecoveryHandle {
    pub(crate) core: Arc<RecoveryCore>,
}

impl RecoveryHandle {
    pub(crate) fn new(core: Arc<RecoveryCore>) -> Self {
        Self { core }
    }

    pub fn id(&self) -> RecoveryId {
        self.core.id()
    }

    pub fn snapshot(&self) -> RecoverySnapshot {
        self.core.snapshot()
    }

    pub fn try_retry(&self) -> Result<RecoveryAttempt, crate::v02::report::RetryError> {
        self.core.try_retry()
    }

    pub fn transfer_to(
        &self,
        destination: &RecoveryStore,
    ) -> Result<RecoveryTransfer, RecoveryTransferError> {
        destination
            .accept(Arc::clone(&self.core))
            .map_err(|(error, _owner)| match error {
                RecoveryStoreError::Closed => RecoveryTransferError::Closed,
                RecoveryStoreError::GroupCapacity | RecoveryStoreError::ResourceCapacity => {
                    RecoveryTransferError::CapacityExceeded
                }
                RecoveryStoreError::AlreadyTransferred => RecoveryTransferError::AlreadyTransferred,
                RecoveryStoreError::InvalidGroup => RecoveryTransferError::InvalidGroup,
            })
    }
}

impl fmt::Debug for RecoveryHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RecoveryHandle").field(&self.id()).finish()
    }
}
impl PartialEq for RecoveryHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
impl Eq for RecoveryHandle {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryStoreError {
    Closed,
    GroupCapacity,
    ResourceCapacity,
    AlreadyTransferred,
    InvalidGroup,
}

struct RecoveryStoreState {
    accepting: bool,
    groups: HashMap<RecoveryId, Arc<RecoveryCore>>,
}

/// Bounded receiver for complete recovery dependency groups.
#[derive(Clone)]
pub struct RecoveryStore {
    id: RecoveryStoreId,
    group_capacity: usize,
    resource_capacity: usize,
    state: Arc<Mutex<RecoveryStoreState>>,
    executor: Arc<crate::v02::executor::CleanupExecutor>,
    async_workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    shutdown: Arc<(Mutex<RecoveryStoreShutdownState>, std::sync::Condvar)>,
    shutdown_wakers: Arc<Mutex<Vec<std::task::Waker>>>,
}

struct RecoveryStoreShutdownState {
    started: bool,
    finished: bool,
}

impl RecoveryStore {
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
        if options.cleanup_executor.worker_count == 0
            || options.cleanup_executor.queue_capacity == 0
        {
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

    #[cfg(test)]
    fn with_capacity(group_capacity: usize, resource_capacity: usize) -> Option<Self> {
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
        let workers = executor
            .start_async_workers(1, "qubit-operation-recovery-test")
            .ok()?;
        Some(Self::from_parts(
            group_capacity,
            resource_capacity,
            executor,
            workers,
        ))
    }

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
                    finished: false,
                }),
                std::sync::Condvar::new(),
            )),
            shutdown_wakers: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn accept(
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
        let nonce = NEXT_RECOVERY_TRANSFER.fetch_add(1, Ordering::Relaxed);
        let transfer = RecoveryTransfer {
            recovery: group.id(),
            receiver: self.id,
            receipt: RecoveryTransferReceipt::new(group.id(), self.id, nonce),
        };
        drop(state);
        group.notify_terminal(false);
        let wakers = Arc::downgrade(&self.shutdown_wakers);
        group.set_terminal_callback(Arc::new(move |_| {
            if let Some(wakers) = wakers.upgrade() {
                for waker in std::mem::take(&mut *lock(&wakers)) {
                    waker.wake();
                }
            }
        }));
        Ok(transfer)
    }

    pub fn contains(&self, id: RecoveryId) -> bool {
        self.prune_resolved();
        lock(&self.state).groups.contains_key(&id)
    }

    pub fn try_retry(
        &self,
        id: RecoveryId,
    ) -> Result<RecoveryAttempt, crate::v02::report::RetryError> {
        let group = lock(&self.state)
            .groups
            .get(&id)
            .cloned()
            .ok_or(crate::v02::report::RetryError::AlreadyResolved)?;
        group.restore_from_store()?;
        group.try_retry()
    }

    pub fn close_admission(&self) {
        lock(&self.state).accepting = false;
    }

    pub fn request_shutdown(&self) -> RecoveryStoreShutdownHandle {
        self.close_admission();
        RecoveryStoreShutdownHandle {
            store: self.clone(),
        }
    }

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

    fn prune_resolved(&self) {
        let removed = {
            let mut state = lock(&self.state);
            let resolved = state
                .groups
                .iter()
                .filter_map(|(id, group)| {
                    (group.snapshot().phase == RecoveryPhase::Resolved).then_some(*id)
                })
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

/// Durable observation of a store after its admission has been closed.
#[derive(Clone)]
pub struct RecoveryStoreShutdownHandle {
    store: RecoveryStore,
}
impl RecoveryStoreShutdownHandle {
    pub fn snapshot(&self) -> RecoveryStoreSnapshot {
        self.store.snapshot()
    }
    pub fn is_complete(&self) -> bool {
        self.store
            .shutdown
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .finished
    }

    pub fn wait(&self) -> Result<(), RecoveryStoreShutdownError> {
        while self.store.snapshot().groups != 0 {
            std::thread::sleep(Duration::from_millis(5));
        }
        let (state_lock, changed) = &*self.store.shutdown;
        let mut state = lock(state_lock);
        if state.finished {
            return Ok(());
        }
        if state.started {
            while !state.finished {
                state = changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
            return Ok(());
        }
        state.started = true;
        drop(state);
        let result = self
            .store
            .executor
            .shutdown()
            .wait()
            .map_err(|_| RecoveryStoreShutdownError::WorkerPanicked);
        let workers = std::mem::take(&mut *lock(&self.store.async_workers));
        let mut panicked = false;
        for worker in workers {
            panicked |= worker.join().is_err();
        }
        let mut state = lock(state_lock);
        state.finished = true;
        changed.notify_all();
        if panicked {
            Err(RecoveryStoreShutdownError::WorkerPanicked)
        } else {
            result
        }
    }

    pub async fn wait_async(&self) -> Result<(), RecoveryStoreShutdownError> {
        std::future::poll_fn(|cx| {
            if self.store.snapshot().groups == 0 {
                return std::task::Poll::Ready(self.wait());
            }
            lock(&self.store.shutdown_wakers).push(cx.waker().clone());
            std::task::Poll::Pending
        })
        .await
    }

    pub fn wait_for(&self, timeout: Duration) -> Result<bool, RecoveryStoreShutdownError> {
        let deadline = Instant::now() + timeout;
        while self.store.snapshot().groups != 0 {
            if Instant::now() >= deadline {
                return Ok(false);
            }
            std::thread::sleep(
                Duration::from_millis(5).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
        self.wait()?;
        Ok(true)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryStoreShutdownError {
    WorkerPanicked,
    ExecutorUnavailable,
}

/// One admitted retry. Dropping it without a result leaves the owner in the
/// core and marks the phase Unconfirmed, never Released.
pub struct RecoveryAttempt {
    core: Arc<RecoveryCore>,
    run: Arc<Mutex<Option<RecoveryRun>>>,
    observation: Arc<AttemptObservation>,
}

struct RecoveryRun {
    core: Arc<RecoveryCore>,
    generation: u64,
    payload: Option<RecoveryPayload>,
    action: Arc<dyn RecoveryAction>,
    completed: bool,
    permit: Option<crate::v02::executor::PortablePermit>,
}

#[derive(Default)]
struct AttemptObservation {
    report: Mutex<Option<crate::v02::report::RecoveryAttemptReport>>,
    waker: Mutex<Option<std::task::Waker>>,
}

impl RecoveryAttempt {
    pub fn wait(
        &self,
    ) -> Result<crate::v02::report::RecoveryAttemptReport, crate::v02::report::ObservationError>
    {
        Ok(block_on(self.wait_async()))
    }

    pub fn as_async(&self) -> AsyncRecoveryAttempt {
        AsyncRecoveryAttempt {
            attempt: self.clone_observer(),
        }
    }

    fn clone_observer(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
            run: Arc::clone(&self.run),
            observation: Arc::clone(&self.observation),
        }
    }

    pub async fn wait_async(&self) -> crate::v02::report::RecoveryAttemptReport {
        if let Some(mut run) = lock(&self.run).take() {
            if let Some(permit) = run.permit.take() {
                let observation = Arc::clone(&self.observation);
                permit.spawn(async move {
                    let payload = run.payload.as_ref().expect("attempt owns payload").clone();
                    let result = run.action.retry(payload).await;
                    run.complete(result);
                    publish_attempt(&observation, run.core.snapshot());
                });
            } else {
                let payload = run.payload.as_ref().expect("attempt owns payload").clone();
                let result = run.action.retry(payload).await;
                run.complete(result);
                publish_attempt(&self.observation, self.core.snapshot());
            }
        }
        wait_attempt_async(&self.observation).await
    }

    pub fn finish(&self, result: Result<(), CleanupFailure>) {
        if let Some(mut run) = lock(&self.run).take() {
            run.complete(result);
        }
        publish_attempt(&self.observation, self.core.snapshot());
    }
}

impl RecoveryRun {
    fn complete(&mut self, result: Result<(), CleanupFailure>) {
        let mut released_payload = None;
        let mut released_action = None;
        let mut old_failures = Vec::new();
        {
            let mut state = lock(&self.core.state);
            if state.phase != RecoveryPhase::Running || state.generation != self.generation {
                self.completed = true;
                return;
            }
            match result {
                Ok(()) => {
                    state.phase = RecoveryPhase::Resolved;
                    old_failures = std::mem::take(&mut state.failures);
                    released_action = state.action.take();
                    released_payload = state.payload.take();
                }
                Err(failure) => {
                    state.phase = RecoveryPhase::Pending;
                    state.failures.push(failure);
                }
            }
        }
        self.completed = true;
        self.payload.take();
        // A resource's last Drop may call arbitrary user code.
        drop(released_payload);
        drop(released_action);
        drop(old_failures);
        if self.core.snapshot().phase == RecoveryPhase::Resolved {
            self.core.notify_terminal(false);
        }
    }
}

pub struct AsyncRecoveryAttempt {
    attempt: RecoveryAttempt,
}
impl AsyncRecoveryAttempt {
    pub async fn wait(&self) -> crate::v02::report::RecoveryAttemptReport {
        self.attempt.wait_async().await
    }
    pub fn as_sync(self) -> RecoveryAttempt {
        self.attempt
    }
}

impl Drop for RecoveryRun {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let mut state = lock(&self.core.state);
        if state.phase == RecoveryPhase::Running && state.generation == self.generation {
            state.phase = RecoveryPhase::Unconfirmed;
        }
        drop(state);
        self.payload.take();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn publish_attempt(observation: &AttemptObservation, snapshot: RecoverySnapshot) {
    *lock(&observation.report) = Some(crate::v02::report::RecoveryAttemptReport {
        failures: Arc::clone(&snapshot.last_failures),
        recovery: snapshot,
    });
    if let Some(waker) = lock(&observation.waker).take() {
        waker.wake();
    }
}

async fn wait_attempt_async(
    observation: &AttemptObservation,
) -> crate::v02::report::RecoveryAttemptReport {
    std::future::poll_fn(|cx| {
        if let Some(report) = lock(&observation.report).clone() {
            return std::task::Poll::Ready(report);
        }
        *lock(&observation.waker) = Some(cx.waker().clone());
        if let Some(report) = lock(&observation.report).clone() {
            std::task::Poll::Ready(report)
        } else {
            std::task::Poll::Pending
        }
    })
    .await
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    struct ThreadWake(std::thread::Thread);
    impl std::task::Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        std::thread::park();
    }
}

fn store_usage(state: &RecoveryStoreState) -> (usize, usize) {
    state
        .groups
        .values()
        .map(|group| {
            let phase = group.snapshot().phase;
            let active = phase != RecoveryPhase::Resolved;
            (
                usize::from(active),
                if active { group.resource_count() } else { 0 },
            )
        })
        .fold((0, 0), |(groups, resources), (group, resource)| {
            (groups + group, resources + resource)
        })
}

#[cfg(test)]
mod tests {
    use super::{RecoveryAction, RecoveryCore, RecoveryPhase, lock};
    use crate::v02::report::{
        CleanupFailure, CleanupStage, DiagnosticSummary, ManagerId, ResourceId,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    struct Noop;
    impl RecoveryAction for Noop {
        fn retry(
            &self,
            _payload: Arc<dyn std::any::Any + Send + Sync>,
        ) -> Pin<Box<dyn Future<Output = Result<(), CleanupFailure>> + Send + 'static>> {
            Box::pin(std::future::ready(Ok(())))
        }
    }

    struct Probe(Arc<AtomicUsize>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Gate {
        state: Mutex<(bool, Option<Waker>)>,
        started: std::sync::mpsc::Sender<()>,
    }
    impl Gate {
        fn open(&self) {
            let waker = {
                let mut state = lock(&self.state);
                state.0 = true;
                state.1.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
        async fn wait(&self) {
            std::future::poll_fn(|cx| {
                let mut state = lock(&self.state);
                if state.0 {
                    Poll::Ready(())
                } else {
                    state.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await
        }
    }

    struct FlakyCleanup {
        calls: AtomicUsize,
        gate: Arc<Gate>,
    }
    impl super::super::cleanup::AsyncCleanup<Probe> for FlakyCleanup {
        fn cleanup(
            self: Arc<Self>,
            resource: Probe,
            _context: crate::v02::context::CleanupContext,
        ) -> super::super::cleanup::CleanupFuture<Probe> {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            let gate = Arc::clone(&self.gate);
            Box::pin(async move {
                if attempt == 0 {
                    ErrCleanup::failed(resource)
                } else {
                    let _ = gate.started.send(());
                    gate.wait().await;
                    drop(resource);
                    super::super::cleanup::CleanupOutcome::Released
                }
            })
        }
    }

    #[test]
    fn cancelled_retry_observer_keeps_real_owner_and_policy_on_store_worker() {
        use crate::v02::manager::capacity::{ManagerCapacityPools, ScopeCapacity};
        use crate::v02::report::{ManagerLimits, ScopeLimits};

        struct NoopWake;
        impl std::task::Wake for NoopWake {
            fn wake(self: Arc<Self>) {}
        }

        let manager = ManagerId::new_unique();
        let resource_id = ResourceId::new(manager, 9, 1);
        let drops = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let gate = Arc::new(Gate {
            state: Mutex::new((false, None)),
            started: started_tx,
        });
        let policy = Arc::new(FlakyCleanup {
            calls: AtomicUsize::new(0),
            gate: Arc::clone(&gate),
        });
        let policy_owner = Arc::clone(&policy);
        let policy: Arc<dyn super::super::cleanup::AsyncCleanup<Probe>> = policy;
        let pools = ManagerCapacityPools::new(ManagerLimits::default());
        let registration = ScopeCapacity::root(&pools, 81, ScopeLimits::default())
            .resource_registration()
            .unwrap();
        let (payload, action) =
            super::async_owner(resource_id, Probe(Arc::clone(&drops)), policy, registration);
        let group = Arc::new(RecoveryCore::new(vec![resource_id], payload, action));
        let handle = super::RecoveryHandle::new(Arc::clone(&group));
        let store = super::RecoveryStore::new(super::RecoveryStoreOptions::default()).unwrap();
        handle.transfer_to(&store).unwrap();

        let first = handle.try_retry().unwrap().wait().unwrap();
        assert_eq!(first.recovery.phase, RecoveryPhase::Pending);
        assert_eq!(handle.snapshot().attempts, 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        let attempt = handle.try_retry().unwrap();
        let mut observer = Box::pin(attempt.wait_async());
        let waker = std::task::Waker::from(Arc::new(NoopWake));
        let mut cx = Context::from_waker(&waker);
        assert!(observer.as_mut().poll(&mut cx).is_pending());
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("store worker starts retry");
        drop(observer);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        gate.open();

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while group.snapshot().phase != RecoveryPhase::Resolved {
            assert!(
                std::time::Instant::now() < deadline,
                "store retry did not finish"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(policy_owner.calls.load(Ordering::SeqCst), 2);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(
            store
                .request_shutdown()
                .wait_for(Duration::from_secs(2))
                .unwrap()
        );
    }

    struct ErrCleanup;
    impl ErrCleanup {
        fn failed(resource: Probe) -> super::super::cleanup::CleanupOutcome<Probe> {
            super::super::cleanup::CleanupOutcome::Failed {
                resource,
                failure: CleanupFailure {
                    resource_id: ResourceId::new(ManagerId::new_unique(), 1, 1),
                    stage: CleanupStage::AsyncRelease,
                    summary: DiagnosticSummary::new("retry me"),
                    source: None,
                },
            }
        }
    }

    fn group() -> Arc<RecoveryCore> {
        Arc::new(RecoveryCore::new(
            vec![ResourceId::new(ManagerId::new_unique(), 1, 1)],
            Arc::new(()),
            Arc::new(Noop),
        ))
    }

    #[test]
    fn public_store_transfer_retains_group_and_returns_signed_receipt() {
        let store = super::RecoveryStore::new(super::RecoveryStoreOptions::default()).unwrap();
        let handle = super::RecoveryHandle::new(group());
        let id = handle.id();
        let transfer = handle.transfer_to(&store).unwrap();
        assert_eq!(transfer.recovery, id);
        assert_eq!(transfer.receipt.recovery(), id);
        assert!(store.contains(id));
        assert_eq!(
            handle.snapshot().phase,
            crate::v02::report::RecoveryPhase::Transferred
        );
        assert_eq!(store.snapshot().groups, 1);
        let shutdown = store.request_shutdown();
        assert!(!shutdown.wait_for(Duration::ZERO).unwrap());
        let attempt = store.try_retry(id).unwrap();
        let report = attempt.wait().unwrap();
        assert_eq!(
            report.recovery.phase,
            crate::v02::report::RecoveryPhase::Resolved
        );
        assert!(shutdown.wait_for(Duration::from_secs(2)).unwrap());
        assert!(shutdown.is_complete());
    }

    #[test]
    fn retry_is_single_flight() {
        let group = group();
        let attempt = group.try_retry().unwrap();
        assert!(group.try_retry().is_err());
        attempt.finish(Ok(()));
        assert!(group.try_retry().is_err());
    }

    #[test]
    fn old_report_is_immutable() {
        let group = group();
        let old = group.snapshot();
        let attempt = group.try_retry().unwrap();
        attempt.finish(Err(CleanupFailure {
            resource_id: old.resources[0],
            stage: CleanupStage::AsyncRelease,
            summary: DiagnosticSummary::new("temporary failure"),
            source: None,
        }));
        assert_eq!(old.attempts, 0);
        assert_eq!(old.phase, crate::v02::report::RecoveryPhase::Pending);
        assert_eq!(group.snapshot().attempts, 1);
    }

    #[test]
    fn unconfirmed_does_not_publish_released() {
        let group = group();
        let attempt = group.try_retry().unwrap();
        drop(attempt);
        assert_eq!(
            group.snapshot().phase,
            crate::v02::report::RecoveryPhase::Unconfirmed
        );
    }

    #[test]
    fn recovery_store_handoff_is_bounded_and_signed() {
        let manager = ManagerId::new_unique();
        let group = Arc::new(RecoveryCore::new(
            vec![
                ResourceId::new(manager, 1, 1),
                ResourceId::new(manager, 2, 1),
            ],
            Arc::new(()),
            Arc::new(Noop),
        ));
        let too_small = super::RecoveryStore::with_capacity(1, 1).unwrap();
        assert!(too_small.accept(Arc::clone(&group)).is_err());
        let handle = super::RecoveryHandle::new(Arc::clone(&group));
        assert_eq!(
            handle.transfer_to(&too_small),
            Err(crate::v02::report::RecoveryTransferError::CapacityExceeded)
        );
        assert_eq!(
            group.snapshot().phase,
            crate::v02::report::RecoveryPhase::Pending
        );

        let store = super::RecoveryStore::with_capacity(1, 2).unwrap();
        let transfer = match store.accept(Arc::clone(&group)) {
            Ok(transfer) => transfer,
            Err((error, _)) => panic!("unexpected store rejection: {error:?}"),
        };
        assert_eq!(transfer.recovery, group.id());
        assert_eq!(transfer.receiver, transfer.receipt.receiver());
        assert!(store.contains(group.id()));
        assert_eq!(
            group.snapshot().phase,
            crate::v02::report::RecoveryPhase::Transferred
        );

        let attempt = store.try_retry(group.id()).unwrap();
        assert_eq!(
            group.snapshot().phase,
            crate::v02::report::RecoveryPhase::Running
        );
        attempt.finish(Ok(()));
        assert_eq!(
            group.snapshot().phase,
            crate::v02::report::RecoveryPhase::Resolved
        );
    }
}

//! Strongly retained recovery payloads and single-flight retry state.
#![allow(dead_code)] // Store admission and manager wiring are completed by T11.

use std::any::Any;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::v02::report::{
    CleanupFailure, RecoveryId, RecoveryPhase, RecoverySnapshot, RecoveryStoreId,
    RecoveryStoreSnapshot, RecoveryTransfer, RecoveryTransferError, RecoveryTransferReceipt,
    ResourceId,
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
            generation,
            payload: Some(payload),
            action,
            completed: false,
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
pub struct RecoveryStoreOptions {
    pub group_capacity: usize,
    pub resource_capacity: usize,
    pub cleanup_concurrency: usize,
    pub cleanup_executor: crate::v02::report::ThreadPoolExecutorOptions,
}
impl Default for RecoveryStoreOptions {
    fn default() -> Self {
        Self {
            group_capacity: 64,
            resource_capacity: 128,
            cleanup_concurrency: 4,
            cleanup_executor: crate::v02::report::ThreadPoolExecutorOptions::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryStoreError {
    Closed,
    GroupCapacity,
    ResourceCapacity,
    AlreadyTransferred,
    InvalidGroup,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryStoreCreateError {
    InvalidOptions,
    ExecutorUnavailable,
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
}

impl RecoveryStore {
    pub fn new(options: RecoveryStoreOptions) -> Result<Self, RecoveryStoreCreateError> {
        if options.group_capacity == 0 {
            return Err(RecoveryStoreCreateError::InvalidOptions);
        }
        if options.resource_capacity == 0 {
            return Err(RecoveryStoreCreateError::InvalidOptions);
        }
        if options.cleanup_concurrency == 0 {
            return Err(RecoveryStoreCreateError::InvalidOptions);
        }
        if options.cleanup_executor.worker_count == 0
            || options.cleanup_executor.queue_capacity == 0
        {
            return Err(RecoveryStoreCreateError::InvalidOptions);
        }
        Self::with_capacity(options.group_capacity, options.resource_capacity)
            .ok_or(RecoveryStoreCreateError::InvalidOptions)
    }

    pub(crate) fn with_capacity(group_capacity: usize, resource_capacity: usize) -> Option<Self> {
        if group_capacity == 0 || resource_capacity == 0 {
            return None;
        }
        Some(Self {
            id: RecoveryStoreId::new_unique(),
            group_capacity,
            resource_capacity,
            state: Arc::new(Mutex::new(RecoveryStoreState {
                accepting: true,
                groups: HashMap::with_capacity(group_capacity),
            })),
        })
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
        state.groups.insert(group.id(), Arc::clone(&group));
        let nonce = NEXT_RECOVERY_TRANSFER.fetch_add(1, Ordering::Relaxed);
        let transfer = RecoveryTransfer {
            recovery: group.id(),
            receiver: self.id,
            receipt: RecoveryTransferReceipt::new(group.id(), self.id, nonce),
        };
        drop(state);
        group.notify_terminal(false);
        Ok(transfer)
    }

    pub fn contains(&self, id: RecoveryId) -> bool {
        self.prune_resolved();
        lock(&self.state).groups.contains_key(&id)
    }

    pub(crate) fn try_retry(
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
        self.store.snapshot().groups == 0
    }
}

/// One admitted retry. Dropping it without a result leaves the owner in the
/// core and marks the phase Unconfirmed, never Released.
pub struct RecoveryAttempt {
    core: Arc<RecoveryCore>,
    generation: u64,
    payload: Option<RecoveryPayload>,
    action: Arc<dyn RecoveryAction>,
    completed: bool,
}

impl RecoveryAttempt {
    pub async fn wait(mut self) -> crate::v02::report::RecoveryAttemptReport {
        let payload = self.payload.as_ref().expect("attempt owns payload").clone();
        let result = self.action.retry(payload).await;
        self.complete(result);
        crate::v02::report::RecoveryAttemptReport {
            recovery: self.core.snapshot(),
            failures: self.core.snapshot().last_failures,
        }
    }

    pub(crate) fn as_async(self) -> AsyncRecoveryAttempt {
        AsyncRecoveryAttempt {
            attempt: Some(self),
        }
    }

    pub fn finish(mut self, result: Result<(), CleanupFailure>) {
        self.complete(result);
    }

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
    attempt: Option<RecoveryAttempt>,
}
impl AsyncRecoveryAttempt {
    pub async fn wait(&mut self) -> crate::v02::report::RecoveryAttemptReport {
        self.attempt
            .take()
            .expect("attempt is observed once")
            .wait()
            .await
    }
    pub fn as_sync(self) -> RecoveryAttempt {
        self.attempt.expect("attempt is present")
    }
}

impl Drop for RecoveryAttempt {
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
    use super::{RecoveryAction, RecoveryCore};
    use crate::v02::report::{
        CleanupFailure, CleanupStage, DiagnosticSummary, ManagerId, ResourceId,
    };
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;

    struct Noop;
    impl RecoveryAction for Noop {
        fn retry(
            &self,
            _payload: Arc<dyn std::any::Any + Send + Sync>,
        ) -> Pin<Box<dyn Future<Output = Result<(), CleanupFailure>> + Send + 'static>> {
            Box::pin(std::future::ready(Ok(())))
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

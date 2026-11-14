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
    RecoveryStoreSnapshot, RecoveryTransfer, RecoveryTransferReceipt, ResourceId,
};

static NEXT_RECOVERY_TRANSFER: AtomicU64 = AtomicU64::new(1);

pub(crate) type RecoveryPayload = Arc<dyn Any + Send + Sync + 'static>;
pub(crate) type RecoveryFuture =
    Pin<Box<dyn Future<Output = Result<(), CleanupFailure>> + Send + 'static>>;

/// Retry strategy operates through a shared retained owner capsule. The
/// capsule remains in `RecoveryCore` across failures and future cancellation.
pub(crate) trait RecoveryAction: Send + Sync + 'static {
    fn retry(&self, payload: RecoveryPayload) -> RecoveryFuture;
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
        true
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
pub(crate) struct RecoveryStore {
    id: RecoveryStoreId,
    group_capacity: usize,
    resource_capacity: usize,
    state: Mutex<RecoveryStoreState>,
}

impl RecoveryStore {
    pub(crate) fn new(group_capacity: usize, resource_capacity: usize) -> Option<Self> {
        if group_capacity == 0 || resource_capacity == 0 {
            return None;
        }
        Some(Self {
            id: RecoveryStoreId::new_unique(),
            group_capacity,
            resource_capacity,
            state: Mutex::new(RecoveryStoreState {
                accepting: true,
                groups: HashMap::with_capacity(group_capacity),
            }),
        })
    }

    pub(crate) fn accept(
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
        Ok(RecoveryTransfer {
            recovery: group.id(),
            receiver: self.id,
            receipt: RecoveryTransferReceipt::new(group.id(), self.id, nonce),
        })
    }

    pub(crate) fn contains(&self, id: RecoveryId) -> bool {
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

    pub(crate) fn close_admission(&self) {
        lock(&self.state).accepting = false;
    }

    pub(crate) fn snapshot(&self) -> RecoveryStoreSnapshot {
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

/// One admitted retry. Dropping it without a result leaves the owner in the
/// core and marks the phase Unconfirmed, never Released.
pub(crate) struct RecoveryAttempt {
    core: Arc<RecoveryCore>,
    generation: u64,
    payload: Option<RecoveryPayload>,
    action: Arc<dyn RecoveryAction>,
    completed: bool,
}

impl RecoveryAttempt {
    pub(crate) async fn run(mut self) {
        let payload = self.payload.as_ref().expect("attempt owns payload").clone();
        let result = self.action.retry(payload).await;
        self.complete(result);
    }

    pub(crate) fn finish(mut self, result: Result<(), CleanupFailure>) {
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
        let too_small = super::RecoveryStore::new(1, 1).unwrap();
        assert!(too_small.accept(Arc::clone(&group)).is_err());
        assert_eq!(
            group.snapshot().phase,
            crate::v02::report::RecoveryPhase::Pending
        );

        let store = super::RecoveryStore::new(1, 2).unwrap();
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

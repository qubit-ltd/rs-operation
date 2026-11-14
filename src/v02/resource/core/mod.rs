//! Synchronization primitives shared by typed resource handles.

use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Mutex};

use super::ResourceRef;
use super::resource_error::{ResourceCommitError, ResourceCommitWithError};
use super::resource_reservation::ResourceReservation;
use crate::v02::executor::CleanupExecutor;
use crate::v02::manager::capacity::{ResourceRegistration, ScopeCapacity};
use crate::v02::report::{
    CleanupEvidence, CleanupFailure, CleanupStage, DiagnosticSummary, ManagerId,
    ResourceCapacityError, ResourceCleanupReport, ResourceId, ResourceManageErrorKind,
    ResourceProgress, ResourceScopeId, ResourceScopeReport, TransferReceipt,
};
use crate::v02::resource::cleanup_port::{CleanupDispatch, CleanupDispatchPermit, CleanupJob};

pub(super) struct Slot<T> {
    key_generation: u64,
    value: Option<Arc<T>>,
    users: usize,
    open: bool,
    registration: Option<ResourceRegistration>,
    cleanup_permit: Option<Box<dyn CleanupDispatchPermit>>,
    resource_id: ResourceId,
    completion: Arc<ScopeCompletion>,
    policy: Option<Arc<dyn super::cleanup::AsyncCleanup<T>>>,
}

pub(super) struct Owner<T> {
    state: Mutex<Slot<T>>,
}
impl<T: Send + Sync + 'static> Owner<T> {
    fn new(
        value: T,
        generation: u64,
        registration: ResourceRegistration,
        cleanup_permit: Box<dyn CleanupDispatchPermit>,
        resource_id: ResourceId,
        completion: Arc<ScopeCompletion>,
        policy: Option<Arc<dyn super::cleanup::AsyncCleanup<T>>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Slot {
                key_generation: generation,
                value: Some(Arc::new(value)),
                users: 0,
                open: true,
                registration: Some(registration),
                cleanup_permit: Some(cleanup_permit),
                resource_id,
                completion,
                policy,
            }),
        })
    }
    pub(super) fn acquire(
        self: &Arc<Self>,
        generation: u64,
    ) -> Result<Arc<Self>, crate::v02::report::ResourceLookupError> {
        let mut state = lock(&self.state);
        if state.key_generation != generation {
            return Err(crate::v02::report::ResourceLookupError::StaleKey);
        }
        if !state.open || state.value.is_none() {
            return Err(crate::v02::report::ResourceLookupError::ScopeEnded);
        }
        state.users += 1;
        Ok(Arc::clone(self))
    }
    pub(super) fn clone_user(
        self: &Arc<Self>,
        generation: u64,
    ) -> Result<Arc<Self>, crate::v02::report::ResourceLookupError> {
        let mut state = lock(&self.state);
        if state.key_generation != generation {
            return Err(crate::v02::report::ResourceLookupError::StaleKey);
        }
        if state.users == 0 || state.value.is_none() {
            return Err(crate::v02::report::ResourceLookupError::ScopeEnded);
        }
        state.users += 1;
        Ok(Arc::clone(self))
    }
    pub(super) fn value_arc(&self) -> Option<Arc<T>> {
        lock(&self.state).value.as_ref().map(Arc::clone)
    }
    pub(super) fn release(&self) {
        let ready = {
            let mut state = lock(&self.state);
            state.users -= 1;
            Self::take_if_ready(&mut state)
        };
        Self::dispatch(ready);
    }
    pub(super) fn end(&self) {
        let ready = {
            let mut state = lock(&self.state);
            state.open = false;
            Self::take_if_ready(&mut state)
        };
        Self::dispatch(ready);
    }
    fn has_users(&self) -> bool {
        lock(&self.state).users != 0
    }
    fn take_if_ready(
        state: &mut Slot<T>,
    ) -> Option<(
        T,
        ResourceRegistration,
        Box<dyn CleanupDispatchPermit>,
        ResourceId,
        Arc<ScopeCompletion>,
        Option<Arc<dyn super::cleanup::AsyncCleanup<T>>>,
    )> {
        if state.open || state.users != 0 {
            return None;
        }
        let value = Arc::into_inner(state.value.take().expect("ready owner retains value"))
            .expect("resource user count matches strong references");
        Some((
            value,
            state
                .registration
                .take()
                .expect("owner holds resource registration"),
            state
                .cleanup_permit
                .take()
                .expect("owner holds cleanup dispatch permit"),
            state.resource_id,
            Arc::clone(&state.completion),
            state.policy.take(),
        ))
    }
    fn dispatch(
        ready: Option<(
            T,
            ResourceRegistration,
            Box<dyn CleanupDispatchPermit>,
            ResourceId,
            Arc<ScopeCompletion>,
            Option<Arc<dyn super::cleanup::AsyncCleanup<T>>>,
        )>,
    ) {
        if let Some((value, registration, permit, resource_id, completion, policy)) = ready {
            completion.start();
            if let Some(policy) = policy {
                let context = crate::v02::context::CleanupContext::new(Default::default())
                    .expect("empty cleanup context is valid");
                let result = Arc::new(Mutex::new(None));
                let completion_for_job = Arc::clone(&completion);
                permit.enqueue(Box::new(
                    super::cleanup::AsyncCleanupJob::new(value, policy, context, result)
                        .with_registration(registration)
                        .with_callback(move |result| {
                            finish_async_cleanup(resource_id, completion_for_job, result);
                        }),
                ));
            } else {
                permit.enqueue(Box::new(DefaultDropJob {
                    value: Some(value),
                    registration: Some(registration),
                    resource_id,
                    completion,
                }));
            }
        }
    }
}

/// Settles a resource scope only after every handed-off resource owner reports.
pub(super) struct ScopeCompletion {
    state: Mutex<ScopeCompletionState>,
}

struct ScopeCompletionState {
    expected: Option<usize>,
    reservations_released_before_seal: usize,
    started: usize,
    completed: usize,
    seen: HashSet<ResourceId>,
    resources: Vec<ResourceCleanupReport>,
    failures: Vec<CleanupFailure>,
    recoveries: Vec<crate::v02::report::RecoveryHandle>,
    recovery_callback:
        Option<Arc<dyn Fn(crate::v02::report::RecoveryHandle, bool) + Send + Sync + 'static>>,
    transfers: Vec<TransferReceipt>,
    transfers_total: usize,
    transfers_truncated: bool,
    callback: Option<Box<dyn FnOnce(ResourceScopeReport) + Send + 'static>>,
    progress: ResourceProgress,
    progress_callback: Option<Arc<dyn Fn(ResourceProgress) + Send + Sync + 'static>>,
}

impl ScopeCompletion {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ScopeCompletionState {
                expected: None,
                reservations_released_before_seal: 0,
                started: 0,
                completed: 0,
                seen: HashSet::new(),
                resources: Vec::new(),
                failures: Vec::new(),
                recoveries: Vec::new(),
                recovery_callback: None,
                transfers: Vec::new(),
                transfers_total: 0,
                transfers_truncated: false,
                callback: None,
                progress: ResourceProgress::ScopeOpen,
                progress_callback: None,
            }),
        })
    }

    pub(super) fn set_progress_callback(
        &self,
        callback: Arc<dyn Fn(ResourceProgress) + Send + Sync + 'static>,
    ) {
        let progress = {
            let mut state = lock(&self.state);
            state.progress_callback = Some(Arc::clone(&callback));
            state.progress
        };
        callback(progress);
    }

    fn advance_progress(&self, progress: ResourceProgress) {
        let callback = {
            let mut state = lock(&self.state);
            if progress_rank(progress) > progress_rank(state.progress) {
                state.progress = progress;
            }
            state
                .progress_callback
                .as_ref()
                .map(|callback| (Arc::clone(callback), state.progress))
        };
        if let Some((callback, progress)) = callback {
            callback(progress);
        }
    }

    fn set_recovery_callback(
        &self,
        callback: Arc<dyn Fn(crate::v02::report::RecoveryHandle, bool) + Send + Sync + 'static>,
    ) {
        let existing = {
            let mut state = lock(&self.state);
            state.recovery_callback = Some(Arc::clone(&callback));
            state.recoveries.clone()
        };
        for handle in existing {
            let terminal = Arc::clone(&callback);
            let terminal_handle = handle.clone();
            handle.core.set_terminal_callback(Arc::new(move |present| {
                terminal(terminal_handle.clone(), present);
            }));
            callback(handle, true);
        }
    }

    pub(super) fn register_recovery(&self, handle: crate::v02::report::RecoveryHandle) {
        let callback = {
            let mut state = lock(&self.state);
            state.recoveries.push(handle.clone());
            state.recovery_callback.clone()
        };
        if let Some(callback) = callback {
            let terminal = Arc::clone(&callback);
            let terminal_handle = handle.clone();
            handle.core.set_terminal_callback(Arc::new(move |present| {
                terminal(terminal_handle.clone(), present);
            }));
            callback(handle, true);
        }
    }

    pub(super) fn set_callback(
        &self,
        callback: Box<dyn FnOnce(ResourceScopeReport) + Send + 'static>,
    ) {
        let (callback, report) = {
            let mut state = lock(&self.state);
            assert!(
                state.callback.is_none(),
                "resource scope callback is one-shot"
            );
            if Self::ready(&state) {
                (Some(callback), Some(Self::report(&state)))
            } else {
                state.callback = Some(callback);
                (None, None)
            }
        };
        if let (Some(callback), Some(report)) = (callback, report) {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| callback(report)));
        }
    }

    pub(super) fn record_transfer(&self, receipt: TransferReceipt) -> bool {
        let mut state = lock(&self.state);
        if state.expected.is_some() {
            return false;
        }
        state.transfers_total = state.transfers_total.saturating_add(1);
        if state.transfers.len() < 256 {
            state.transfers.push(receipt);
        } else {
            state.transfers_truncated = true;
        }
        true
    }

    fn start(&self) {
        {
            let mut state = lock(&self.state);
            state.started = state.started.saturating_add(1);
        }
        self.advance_progress(ResourceProgress::Cleaning);
    }

    fn waiting_for_users(&self) {
        self.advance_progress(ResourceProgress::WaitingForUsers);
    }

    fn cleaning(&self) -> usize {
        let state = lock(&self.state);
        state.started.saturating_sub(state.completed)
    }

    fn seal(&self, expected: usize) {
        let (callback, report) = {
            let mut state = lock(&self.state);
            state.expected = Some(expected.saturating_sub(state.reservations_released_before_seal));
            state.reservations_released_before_seal = 0;
            if Self::ready(&state) {
                (state.callback.take(), Some(Self::report(&state)))
            } else {
                (None, None)
            }
        };
        if let (Some(callback), Some(report)) = (callback, report) {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| callback(report)));
        }
    }

    fn release_pending_reservation(&self) {
        let (callback, report) = {
            let mut state = lock(&self.state);
            if let Some(expected) = state.expected.as_mut() {
                *expected = expected.saturating_sub(1);
            } else {
                state.reservations_released_before_seal =
                    state.reservations_released_before_seal.saturating_add(1);
            }
            if Self::ready(&state) {
                (state.callback.take(), Some(Self::report(&state)))
            } else {
                (None, None)
            }
        };
        if let (Some(callback), Some(report)) = (callback, report) {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| callback(report)));
        }
    }

    fn complete(
        &self,
        resource_id: ResourceId,
        evidence: Option<CleanupEvidence>,
        failure: Option<CleanupFailure>,
        recovery: Option<crate::v02::report::RecoveryHandle>,
    ) {
        let (callback, report, recovery_callback) = {
            let mut state = lock(&self.state);
            if !state.seen.insert(resource_id) {
                return;
            }
            state.completed = state.completed.saturating_add(1);
            if let Some(evidence) = evidence {
                state.resources.push(ResourceCleanupReport {
                    resource_id,
                    evidence,
                });
            }
            if let Some(failure) = failure {
                state.failures.push(failure);
            }
            let recovery_callback = recovery.as_ref().and_then(|handle| {
                state.recoveries.push(handle.clone());
                state
                    .recovery_callback
                    .clone()
                    .map(|callback| (callback, handle.clone()))
            });
            if Self::ready(&state) {
                (
                    state.callback.take(),
                    Some(Self::report(&state)),
                    recovery_callback,
                )
            } else {
                (None, None, recovery_callback)
            }
        };
        if let Some((callback, handle)) = recovery_callback {
            let terminal = Arc::clone(&callback);
            let terminal_handle = handle.clone();
            handle.core.set_terminal_callback(Arc::new(move |present| {
                terminal(terminal_handle.clone(), present)
            }));
            callback(handle, true);
        }
        if recovery.is_some() {
            self.advance_progress(ResourceProgress::Unconfirmed);
        }
        if let (Some(callback), Some(report)) = (callback, report) {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| callback(report)));
        }
    }

    fn ready(state: &ScopeCompletionState) -> bool {
        state
            .expected
            .is_some_and(|expected| state.completed >= expected)
    }

    fn report(state: &ScopeCompletionState) -> ResourceScopeReport {
        ResourceScopeReport {
            resources: Arc::from(state.resources.clone()),
            failures: Arc::from(state.failures.clone()),
            recoveries: Arc::from(state.recoveries.clone()),
            transfers: Arc::from(state.transfers.clone()),
            transfers_total: state.transfers_total,
            transfers_truncated: state.transfers_truncated,
        }
    }
}

struct DefaultDropJob<T> {
    value: Option<T>,
    registration: Option<ResourceRegistration>,
    resource_id: ResourceId,
    completion: Arc<ScopeCompletion>,
}
impl<T> Drop for DefaultDropJob<T> {
    fn drop(&mut self) {
        // An unrun cleanup job must never destroy its owner on the dropping
        // thread. The reserved dispatcher normally keeps it queued; leaking is
        // the last-resort behavior if that invariant is violated.
        if let Some(value) = self.value.take() {
            std::mem::forget(value);
        }
        if let Some(registration) = self.registration.take() {
            std::mem::forget(registration);
        }
        // Keep the settlement pending when an accepted cleanup job itself is
        // abandoned. No Released fact is synthesized after losing its owner.
    }
}
impl<T: Send + 'static> CleanupJob for DefaultDropJob<T> {
    fn schedule(
        mut self: Box<Self>,
        executor: Arc<CleanupExecutor>,
    ) -> Result<(), Box<dyn CleanupJob>> {
        let permit = match executor.reserve_drop() {
            Ok(permit) => permit,
            Err(_) => return Err(self),
        };
        let value = self.value.take().expect("cleanup job owns resource");
        let registration = self
            .registration
            .take()
            .expect("cleanup job owns registration");
        let scheduled = ScheduledDrop {
            value: Some(value),
            registration: Some(registration),
            resource_id: self.resource_id,
            completion: Arc::clone(&self.completion),
        };
        permit.drop_value(scheduled);
        Ok(())
    }
}

fn finish_async_cleanup<T: Send + Sync + 'static>(
    resource_id: ResourceId,
    completion: Arc<ScopeCompletion>,
    mut result: super::cleanup::AsyncCleanupCompletion<T>,
) {
    use super::cleanup::CleanupOutcome;
    let outcome = result.outcome.take();
    match outcome {
        Some(CleanupOutcome::Released) => {
            drop(result.registration.take());
            completion.complete(resource_id, Some(CleanupEvidence::Confirmed), None, None);
        }
        Some(CleanupOutcome::Transferred(receipt)) => {
            drop(result.registration.take());
            let _ = completion.record_transfer(receipt);
            completion.complete(resource_id, None, None, None);
        }
        Some(CleanupOutcome::Failed { failure, resource }) => {
            let registration = result
                .registration
                .take()
                .expect("failed cleanup retains registration");
            let (payload, action) =
                super::recovery::async_owner(resource_id, resource, result.policy, registration);
            let core = Arc::new(super::recovery::RecoveryCore::new(
                vec![resource_id],
                payload,
                action,
            ));
            completion.complete(
                resource_id,
                None,
                Some(failure),
                Some(crate::v02::report::RecoveryHandle::new(core)),
            );
        }
        None => {
            if let Some(unconfirmed) = result.unconfirmed.take() {
                let payload = Arc::new(Mutex::new(Some(unconfirmed))) as Arc<dyn Any + Send + Sync>;
                let core = Arc::new(super::recovery::RecoveryCore::new_unconfirmed(
                    vec![resource_id],
                    payload,
                ));
                completion.complete(
                    resource_id,
                    None,
                    None,
                    Some(crate::v02::report::RecoveryHandle::new(core)),
                );
            }
        }
    }
}

struct ScheduledDrop<T> {
    value: Option<T>,
    registration: Option<ResourceRegistration>,
    resource_id: ResourceId,
    completion: Arc<ScopeCompletion>,
}

impl<T> Drop for ScheduledDrop<T> {
    fn drop(&mut self) {
        let result = panic::catch_unwind(AssertUnwindSafe(|| drop(self.value.take())));
        let failure = result.as_ref().err().map(|payload| CleanupFailure {
            resource_id: self.resource_id,
            stage: CleanupStage::Drop,
            summary: DiagnosticSummary::new(&panic_summary(payload.as_ref())),
            source: None,
        });
        let evidence = result.is_ok().then_some(CleanupEvidence::DropCompleted);
        let recovery = result.err().map(|panic| {
            let unconfirmed =
                super::cleanup::UnconfirmedCleanup::from_panic(self.registration.take(), panic);
            let payload = Arc::new(Mutex::new(Some(unconfirmed))) as Arc<dyn Any + Send + Sync>;
            let core =
                super::recovery::RecoveryCore::new_unconfirmed(vec![self.resource_id], payload);
            crate::v02::report::RecoveryHandle::new(Arc::new(core))
        });
        if evidence.is_some() {
            drop(self.registration.take());
        }
        self.completion
            .complete(self.resource_id, evidence, failure, recovery);
    }
}

fn panic_summary(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        format!("resource destructor panicked: {message}")
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        format!("resource destructor panicked: {message}")
    } else {
        "resource destructor panicked with a non-string payload".to_owned()
    }
}

pub(super) struct Entry {
    pub(super) owner: Box<dyn ErasedOwner>,
}
pub(super) trait ErasedOwner: Any + Send + Sync {
    fn end(&self);
    fn has_users(&self) -> bool;
    fn as_any(&self) -> &dyn Any;
}
impl<T: Send + Sync + 'static> ErasedOwner for Arc<Owner<T>> {
    fn end(&self) {
        Owner::end(self);
    }
    fn has_users(&self) -> bool {
        Owner::has_users(self)
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
pub(super) struct State {
    pub(super) open: bool,
    pub(super) next_slot: u64,
    pub(super) generation: u64,
    pub(super) reservations: usize,
    pub(super) retained: usize,
    pub(super) slots: HashMap<u64, Entry>,
}

pub(super) struct ScopeCore {
    pub(super) manager: ManagerId,
    pub(super) id: ResourceScopeId,
    capacity: Arc<ScopeCapacity>,
    cleanup: Arc<dyn CleanupDispatch>,
    completion: Arc<ScopeCompletion>,
    pub(super) state: Mutex<State>,
}
impl ScopeCore {
    pub(super) fn reserve(self: &Arc<Self>) -> Result<ResourceReservation, ResourceCapacityError> {
        {
            let state = lock(&self.state);
            if !state.open {
                return Err(ResourceCapacityError::ScopeEnded);
            }
        }
        let registration = self
            .capacity
            .resource_registration()
            .map_err(|_| ResourceCapacityError::CapacityExceeded)?;
        let cleanup_permit = self
            .cleanup
            .reserve()
            .map_err(|_| ResourceCapacityError::CapacityExceeded)?;
        let mut state = lock(&self.state);
        if !state.open {
            drop(state);
            drop(cleanup_permit);
            drop(registration);
            return Err(ResourceCapacityError::ScopeEnded);
        }
        state.reservations = state.reservations.saturating_add(1);
        drop(state);
        Ok(ResourceReservation::new(
            Arc::clone(self),
            registration,
            cleanup_permit,
        ))
    }

    pub(super) fn release_reservation(self: &Arc<Self>) {
        let ended = {
            let mut state = lock(&self.state);
            debug_assert!(state.reservations > 0);
            state.reservations = state.reservations.saturating_sub(1);
            !state.open
        };
        if ended {
            self.completion.release_pending_reservation();
        }
    }
    pub(super) fn commit_reserved<T: Send + Sync + 'static>(
        self: &Arc<Self>,
        value: T,
        mut reservation: ResourceReservation,
    ) -> Result<ResourceRef<T>, ResourceCommitError<T>> {
        let mut state = lock(&self.state);
        let (slot, generation, registration, cleanup_permit) =
            match self.take_commit_parts(&mut state, &mut reservation) {
                Ok(parts) => parts,
                Err(reason) => return Err(ResourceCommitError::new(reason, value, reservation)),
            };
        let resource_id = ResourceId::new(self.manager, slot, generation);
        let owner = Owner::new(
            value,
            generation,
            registration,
            cleanup_permit,
            resource_id,
            Arc::clone(&self.completion),
            None,
        );
        let value = owner.value_arc().expect("new owner contains value");
        let owner_ref = owner.acquire(generation).expect("new owner is open");
        state.slots.insert(
            slot,
            Entry {
                owner: Box::new(owner),
            },
        );
        Ok(ResourceRef {
            owner: owner_ref,
            value: Some(value),
            generation,
            manager: self.manager,
            scope: self.id,
            slot,
        })
    }

    pub(super) fn commit_reserved_with<T, C>(
        self: &Arc<Self>,
        value: T,
        cleanup: C,
        mut reservation: ResourceReservation,
    ) -> Result<ResourceRef<T>, ResourceCommitWithError<T, C>>
    where
        T: Send + Sync + 'static,
        C: super::cleanup::AsyncCleanup<T>,
    {
        let mut state = lock(&self.state);
        let (slot, generation, registration, cleanup_permit) =
            match self.take_commit_parts(&mut state, &mut reservation) {
                Ok(parts) => parts,
                Err(reason) => {
                    return Err(ResourceCommitWithError::new(
                        reason,
                        value,
                        cleanup,
                        reservation,
                    ));
                }
            };
        let resource_id = ResourceId::new(self.manager, slot, generation);
        let policy: Arc<dyn super::cleanup::AsyncCleanup<T>> = Arc::new(cleanup);
        let owner = Owner::new(
            value,
            generation,
            registration,
            cleanup_permit,
            resource_id,
            Arc::clone(&self.completion),
            Some(policy),
        );
        let value = owner.value_arc().expect("new owner contains value");
        let owner_ref = owner.acquire(generation).expect("new owner is open");
        state.slots.insert(
            slot,
            Entry {
                owner: Box::new(owner),
            },
        );
        Ok(ResourceRef {
            owner: owner_ref,
            value: Some(value),
            generation,
            manager: self.manager,
            scope: self.id,
            slot,
        })
    }

    fn take_commit_parts(
        self: &Arc<Self>,
        state: &mut State,
        reservation: &mut ResourceReservation,
    ) -> Result<
        (
            u64,
            u64,
            ResourceRegistration,
            Box<dyn CleanupDispatchPermit>,
        ),
        ResourceManageErrorKind,
    > {
        if !Arc::ptr_eq(self, &reservation.scope) {
            return Err(ResourceManageErrorKind::ScopeEnded);
        }
        if !state.open || !reservation.tracked {
            return Err(ResourceManageErrorKind::ScopeEnded);
        }
        let slot = state.next_slot;
        let next_slot = slot
            .checked_add(1)
            .ok_or(ResourceManageErrorKind::CapacityExceeded)?;
        let registration = reservation
            .registration
            .take()
            .expect("reservation has resource permit");
        let cleanup_permit = reservation
            .cleanup_permit
            .take()
            .expect("reservation has cleanup slot");
        state.next_slot = next_slot;
        state.reservations = state.reservations.saturating_sub(1);
        reservation.tracked = false;
        Ok((slot, state.generation, registration, cleanup_permit))
    }

    pub(super) fn retain_reserved<T: Send + Sync + 'static>(
        self: &Arc<Self>,
        value: T,
        reservation: ResourceReservation,
    ) -> ResourceId {
        self.retain_reserved_inner(value, None, reservation)
    }

    pub(super) fn retain_reserved_with<T, C>(
        self: &Arc<Self>,
        value: T,
        cleanup: C,
        reservation: ResourceReservation,
    ) -> ResourceId
    where
        T: Send + Sync + 'static,
        C: super::cleanup::AsyncCleanup<T>,
    {
        self.retain_reserved_inner(value, Some(Arc::new(cleanup)), reservation)
    }

    fn retain_reserved_inner<T: Send + Sync + 'static>(
        self: &Arc<Self>,
        value: T,
        policy: Option<Arc<dyn super::cleanup::AsyncCleanup<T>>>,
        mut reservation: ResourceReservation,
    ) -> ResourceId {
        assert!(Arc::ptr_eq(self, &reservation.scope));
        let (resource_id, registration, cleanup_permit) = {
            let mut state = lock(&self.state);
            assert!(reservation.tracked, "reservation is consumed once");
            let slot = state.next_slot;
            state.next_slot = slot.checked_add(1).expect("resource slot space exhausted");
            state.reservations = state.reservations.saturating_sub(1);
            state.retained = state.retained.saturating_add(1);
            reservation.tracked = false;
            (
                ResourceId::new(self.manager, slot, state.generation),
                reservation
                    .registration
                    .take()
                    .expect("reservation has resource permit"),
                reservation
                    .cleanup_permit
                    .take()
                    .expect("reservation has cleanup slot"),
            )
        };
        let owner = Owner::new(
            value,
            resource_id.generation(),
            registration,
            cleanup_permit,
            resource_id,
            Arc::clone(&self.completion),
            policy,
        );
        owner.end();
        resource_id
    }
}

/// Synchronization and ownership state for one independently endable scope.
pub(super) struct ScopeState {
    pub(super) core: Arc<ScopeCore>,
    completion: Arc<ScopeCompletion>,
}
impl ScopeState {
    pub(super) fn new(
        manager: ManagerId,
        id: ResourceScopeId,
        capacity: Arc<ScopeCapacity>,
        cleanup: Arc<dyn CleanupDispatch>,
    ) -> Arc<Self> {
        let completion = ScopeCompletion::new();
        Arc::new(Self {
            core: Arc::new(ScopeCore {
                manager,
                id,
                capacity,
                cleanup,
                completion: Arc::clone(&completion),
                state: Mutex::new(State {
                    open: true,
                    next_slot: 1,
                    generation: 1,
                    reservations: 0,
                    retained: 0,
                    slots: HashMap::new(),
                }),
            }),
            completion,
        })
    }

    pub(super) fn set_settlement_callback(
        &self,
        callback: Box<dyn FnOnce(ResourceScopeReport) + Send + 'static>,
    ) {
        self.completion.set_callback(callback);
    }

    pub(super) fn set_progress_callback(
        &self,
        callback: Arc<dyn Fn(ResourceProgress) + Send + Sync + 'static>,
    ) {
        self.completion.set_progress_callback(callback);
    }

    pub(super) fn set_recovery_callback(
        &self,
        callback: Arc<dyn Fn(crate::v02::report::RecoveryHandle, bool) + Send + Sync + 'static>,
    ) {
        self.completion.set_recovery_callback(callback);
    }

    pub(super) fn record_transfer(&self, receipt: TransferReceipt) -> bool {
        self.completion.record_transfer(receipt)
    }

    pub(super) fn cleanup_in_flight(&self) -> usize {
        self.completion.cleaning()
    }

    pub(super) fn end(&self) {
        let (slots, expected, reservations) = {
            let mut state = lock(&self.core.state);
            if !state.open {
                return;
            }
            state.open = false;
            state.generation = state.generation.wrapping_add(1);
            let expected = state
                .slots
                .len()
                .saturating_add(state.reservations)
                .saturating_add(state.retained);
            (
                std::mem::take(&mut state.slots),
                expected,
                state.reservations,
            )
        };
        let waiting_for_users =
            reservations > 0 || slots.values().any(|entry| entry.owner.has_users());
        if waiting_for_users {
            self.completion.waiting_for_users();
        }
        self.completion.seal(expected);
        for entry in slots.into_values() {
            entry.owner.end();
        }
    }
}
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn progress_rank(progress: ResourceProgress) -> u8 {
    match progress {
        ResourceProgress::ScopeOpen => 0,
        ResourceProgress::WaitingForUsers => 1,
        ResourceProgress::Cleaning => 2,
        ResourceProgress::Unconfirmed => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::{Owner, ScopeCompletion};
    use crate::v02::executor::CleanupExecutor;
    use crate::v02::manager::capacity::ResourceRegistration;
    use crate::v02::manager::capacity::{ManagerCapacityPools, ScopeCapacity};
    use crate::v02::report::{
        ManagerLimits, PortableExecutorOptions, ResourceId, ResourceProgress, ScopeLimits,
        ThreadPoolExecutorOptions,
    };
    use crate::v02::resource::cleanup_port::{
        CleanupDispatch, CleanupDispatchPermit, CleanupJob, CleanupQueueError,
    };
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    struct Slot {
        reserved: bool,
        job: Option<Box<dyn CleanupJob>>,
    }
    struct ManualDispatch {
        slots: Arc<Mutex<Vec<Slot>>>,
    }
    impl ManualDispatch {
        fn new() -> Self {
            Self {
                slots: Arc::new(Mutex::new(
                    (0..4)
                        .map(|_| Slot {
                            reserved: false,
                            job: None,
                        })
                        .collect(),
                )),
            }
        }
        fn reserve_permit(&self) -> Box<dyn CleanupDispatchPermit> {
            let mut slots = self.slots.lock().unwrap();
            let (index, slot) = slots
                .iter_mut()
                .enumerate()
                .find(|(_, slot)| !slot.reserved)
                .unwrap();
            slot.reserved = true;
            Box::new(ManualPermit {
                slots: Arc::clone(&self.slots),
                index,
            })
        }
        fn run_all(&self, executor: &Arc<CleanupExecutor>) {
            let count = self.slots.lock().unwrap().len();
            for index in 0..count {
                let job = self.slots.lock().unwrap()[index].job.take();
                let Some(job) = job else { continue };
                match job.schedule(Arc::clone(executor)) {
                    Ok(()) => self.slots.lock().unwrap()[index].reserved = false,
                    Err(job) => {
                        self.slots.lock().unwrap()[index].job = Some(job);
                        break;
                    }
                }
            }
        }
        fn queued(&self) -> usize {
            self.slots
                .lock()
                .unwrap()
                .iter()
                .filter(|slot| slot.job.is_some())
                .count()
        }
    }
    impl CleanupDispatch for ManualDispatch {
        fn reserve(&self) -> Result<Box<dyn CleanupDispatchPermit>, CleanupQueueError> {
            Ok(self.reserve_permit())
        }
    }
    struct ManualPermit {
        slots: Arc<Mutex<Vec<Slot>>>,
        index: usize,
    }
    impl CleanupDispatchPermit for ManualPermit {
        fn enqueue(self: Box<Self>, job: Box<dyn CleanupJob>) {
            let mut slots = self.slots.lock().unwrap();
            assert!(slots[self.index].reserved && slots[self.index].job.is_none());
            slots[self.index].job = Some(job);
        }
    }
    impl Drop for ManualPermit {
        fn drop(&mut self) {
            let mut slots = self.slots.lock().unwrap();
            if slots[self.index].job.is_none() {
                slots[self.index].reserved = false;
            }
        }
    }

    struct DropMark(Arc<AtomicUsize>);
    impl Drop for DropMark {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn registration() -> ResourceRegistration {
        let pools = ManagerCapacityPools::new(ManagerLimits::default());
        ScopeCapacity::root(&pools, 51, ScopeLimits::default())
            .resource_registration()
            .unwrap()
    }
    fn executor() -> Arc<CleanupExecutor> {
        Arc::new(
            CleanupExecutor::new(
                ThreadPoolExecutorOptions {
                    worker_count: 1,
                    queue_capacity: 8,
                },
                PortableExecutorOptions::default(),
            )
            .unwrap(),
        )
    }
    fn finish(dispatch: &ManualDispatch, executor: &Arc<CleanupExecutor>) {
        dispatch.run_all(executor);
        executor.shutdown().wait().unwrap();
    }
    fn owner<T: Send + Sync + 'static>(
        value: T,
        generation: u64,
        dispatch: &ManualDispatch,
    ) -> Arc<Owner<T>> {
        Owner::new(
            value,
            generation,
            registration(),
            dispatch.reserve_permit(),
            ResourceId::new(
                crate::v02::report::ManagerId::new_unique(),
                generation,
                generation,
            ),
            super::ScopeCompletion::new(),
            None,
        )
    }

    #[test]
    fn reservation_release_before_scope_seal_does_not_strand_settlement() {
        let completion = ScopeCompletion::new();
        let report = Arc::new(Mutex::new(None));
        let observed = Arc::clone(&report);
        completion.set_callback(Box::new(move |value| {
            *observed.lock().unwrap() = Some(value);
        }));

        completion.release_pending_reservation();
        completion.seal(1);

        let report = report.lock().unwrap().take().unwrap();
        assert!(report.resources.is_empty());
        assert!(report.failures.is_empty());
    }

    #[test]
    fn open_with_zero_users_keeps_value() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = owner(DropMark(Arc::clone(&drops)), 1, &dispatch);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(dispatch.queued(), 0);
        owner.end();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        finish(&dispatch, &executor);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn ended_scope_with_live_user_keeps_value() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = owner(DropMark(Arc::clone(&drops)), 1, &dispatch);
        let user = owner.acquire(1).unwrap();
        owner.end();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(dispatch.queued(), 0);
        user.release();
        assert_eq!(dispatch.queued(), 1);
        finish(&dispatch, &executor);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn end_races_last_ref_cleans_once() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = owner(DropMark(Arc::clone(&drops)), 1, &dispatch);
        let user = owner.acquire(1).unwrap();
        let other = Arc::clone(&owner);
        std::thread::scope(|scope| {
            scope.spawn(move || other.end());
            scope.spawn(move || user.release());
        });
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(dispatch.queued(), 1);
        finish(&dispatch, &executor);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn get_races_end_never_returns_dead_value() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let drops = Arc::new(AtomicUsize::new(0));
        let owner = owner(DropMark(Arc::clone(&drops)), 7, &dispatch);
        let ender = Arc::clone(&owner);
        std::thread::scope(|scope| {
            scope.spawn(move || ender.end());
            if let Ok(user) = owner.acquire(7) {
                assert_eq!(drops.load(Ordering::SeqCst), 0);
                user.release();
            }
        });
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(dispatch.queued(), 1);
        finish(&dispatch, &executor);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn old_key_generation_is_rejected() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let owner = owner((), 2, &dispatch);
        assert!(owner.acquire(1).is_err());
        owner.end();
        finish(&dispatch, &executor);
    }

    #[test]
    fn default_drop_panic_retains_unconfirmed_owner_and_report_handle() {
        struct Panics;
        impl Drop for Panics {
            fn drop(&mut self) {
                panic!("release uncertain");
            }
        }
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let completion = ScopeCompletion::new();
        let progress = Arc::new(Mutex::new(Vec::new()));
        let observed_progress = Arc::clone(&progress);
        completion.set_progress_callback(Arc::new(move |value| {
            observed_progress.lock().unwrap().push(value);
        }));
        let report = Arc::new(Mutex::new(None));
        let report_slot = Arc::clone(&report);
        completion.set_callback(Box::new(move |value| {
            *report_slot.lock().unwrap() = Some(value)
        }));
        let owner = Owner::new(
            Panics,
            9,
            registration(),
            dispatch.reserve_permit(),
            ResourceId::new(crate::v02::report::ManagerId::new_unique(), 9, 9),
            Arc::clone(&completion),
            None,
        );
        owner.end();
        completion.seal(1);
        finish(&dispatch, &executor);
        let report = report.lock().unwrap().take().unwrap();
        assert_eq!(report.recoveries.len(), 1);
        assert_eq!(
            *progress.lock().unwrap(),
            vec![
                ResourceProgress::ScopeOpen,
                ResourceProgress::Cleaning,
                ResourceProgress::Unconfirmed
            ]
        );
        assert_eq!(
            report.recoveries[0].snapshot().phase,
            crate::v02::report::RecoveryPhase::Unconfirmed
        );
        assert_eq!(report.failures.len(), 1);
    }

    #[test]
    fn ref_after_end_can_clone_and_read() {
        let dispatch = ManualDispatch::new();
        let executor = executor();
        let owner = owner(String::from("still alive"), 1, &dispatch);
        let held = owner.acquire(1).unwrap();
        owner.end();
        let cloned = held.clone_user(1).unwrap();
        assert_eq!(
            cloned.value_arc().as_deref().map(|value| value.as_str()),
            Some("still alive")
        );
        cloned.release();
        held.release();
        assert_eq!(dispatch.queued(), 1);
        finish(&dispatch, &executor);
    }
}

//! Synchronization primitives shared by typed resource handles.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::ResourceRef;
use super::resource_error::{ResourceCapacityError, ResourceCommitError};
use super::resource_reservation::ResourceReservation;
use crate::v02::executor::CleanupExecutor;
use crate::v02::manager::capacity::{ResourceRegistration, ScopeCapacity};
use crate::v02::report::{ManagerId, ResourceManageErrorKind, ResourceScopeId};
use crate::v02::resource::cleanup_port::{
    CleanupDispatch, CleanupDispatchPermit, CleanupJob, CleanupQueueError,
};

pub(super) struct Slot<T> {
    key_generation: u64,
    value: Option<Arc<T>>,
    users: usize,
    open: bool,
    registration: Option<ResourceRegistration>,
    cleanup_permit: Option<Box<dyn CleanupDispatchPermit>>,
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
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Slot {
                key_generation: generation,
                value: Some(Arc::new(value)),
                users: 0,
                open: true,
                registration: Some(registration),
                cleanup_permit: Some(cleanup_permit),
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
    fn take_if_ready(
        state: &mut Slot<T>,
    ) -> Option<(T, ResourceRegistration, Box<dyn CleanupDispatchPermit>)> {
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
        ))
    }
    fn dispatch(ready: Option<(T, ResourceRegistration, Box<dyn CleanupDispatchPermit>)>) {
        if let Some((value, registration, permit)) = ready {
            permit.enqueue(Box::new(DefaultDropJob {
                value: Some(value),
                registration: Some(registration),
            }));
        }
    }
}

struct DefaultDropJob<T> {
    value: Option<T>,
    registration: Option<ResourceRegistration>,
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
        permit.drop_value((value, registration));
        Ok(())
    }
}

pub(super) struct Entry {
    pub(super) owner: Box<dyn ErasedOwner>,
}
pub(super) trait ErasedOwner: Any + Send + Sync {
    fn end(&self);
    fn as_any(&self) -> &dyn Any;
}
impl<T: Send + Sync + 'static> ErasedOwner for Arc<Owner<T>> {
    fn end(&self) {
        Owner::end(self);
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}
pub(super) struct State {
    pub(super) open: bool,
    pub(super) next_slot: u64,
    pub(super) generation: u64,
    pub(super) slots: HashMap<u64, Entry>,
}

pub(super) struct ScopeCore {
    pub(super) manager: ManagerId,
    pub(super) id: ResourceScopeId,
    capacity: Arc<ScopeCapacity>,
    cleanup: Arc<dyn CleanupDispatch>,
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
            .map_err(|_| ResourceCapacityError::Exhausted)?;
        let cleanup_permit = self.cleanup.reserve().map_err(|error| match error {
            CleanupQueueError::Full => ResourceCapacityError::CleanupQueueFull,
            CleanupQueueError::Closed => ResourceCapacityError::CleanupQueueClosed,
        })?;
        let state = lock(&self.state);
        if !state.open {
            drop(state);
            drop(cleanup_permit);
            drop(registration);
            return Err(ResourceCapacityError::ScopeEnded);
        }
        drop(state);
        Ok(ResourceReservation::new(
            Arc::clone(self),
            registration,
            cleanup_permit,
        ))
    }
    pub(super) fn commit_reserved<T: Send + Sync + 'static>(
        self: &Arc<Self>,
        value: T,
        mut reservation: ResourceReservation,
    ) -> Result<ResourceRef<T>, ResourceCommitError<T>> {
        if !Arc::ptr_eq(self, &reservation.scope) {
            return Err(ResourceCommitError::new(
                ResourceManageErrorKind::ScopeEnded,
                value,
                reservation,
            ));
        }
        let mut state = lock(&self.state);
        if !state.open {
            return Err(ResourceCommitError::new(
                ResourceManageErrorKind::ScopeEnded,
                value,
                reservation,
            ));
        }
        let slot = state.next_slot;
        let Some(next_slot) = slot.checked_add(1) else {
            return Err(ResourceCommitError::new(
                ResourceManageErrorKind::CapacityExceeded,
                value,
                reservation,
            ));
        };
        state.next_slot = next_slot;
        let generation = state.generation;
        let registration = reservation
            .registration
            .take()
            .expect("reservation has resource permit");
        let cleanup_permit = reservation
            .cleanup_permit
            .take()
            .expect("reservation has cleanup slot");
        let owner = Owner::new(value, generation, registration, cleanup_permit);
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
}

/// Synchronization and ownership state for one independently endable scope.
pub(super) struct ScopeState {
    pub(super) core: Arc<ScopeCore>,
}
impl ScopeState {
    pub(super) fn new(
        manager: ManagerId,
        id: ResourceScopeId,
        capacity: Arc<ScopeCapacity>,
        cleanup: Arc<dyn CleanupDispatch>,
    ) -> Arc<Self> {
        Arc::new(Self {
            core: Arc::new(ScopeCore {
                manager,
                id,
                capacity,
                cleanup,
                state: Mutex::new(State {
                    open: true,
                    next_slot: 1,
                    generation: 1,
                    slots: HashMap::new(),
                }),
            }),
        })
    }
    pub(super) fn end(&self) {
        let slots = {
            let mut state = lock(&self.core.state);
            if !state.open {
                return;
            }
            state.open = false;
            state.generation = state.generation.wrapping_add(1);
            std::mem::take(&mut state.slots)
        };
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

#[cfg(test)]
mod tests {
    use super::Owner;
    use crate::v02::executor::CleanupExecutor;
    use crate::v02::manager::capacity::ResourceRegistration;
    use crate::v02::manager::capacity::{ManagerCapacityPools, ScopeCapacity};
    use crate::v02::report::{
        ManagerLimits, PortableExecutorOptions, ScopeLimits, ThreadPoolExecutorOptions,
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
        Owner::new(value, generation, registration(), dispatch.reserve_permit())
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

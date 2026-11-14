//! Bounded capacity and permit accounting.
#![allow(dead_code)] // T04 staging interfaces are consumed by later v0.2 wiring tasks.

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};

use qubit_budget::{ManagedResourcePermit, ManagedResourcePool};

use crate::v02::report::{ManagerLimits, ScopeLimits};

static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_WAITER_ID: AtomicU64 = AtomicU64::new(1);

/// A finite manager or operation-tree capacity backed by `qubit-budget`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResourceLabel {
    Named(&'static str),
    Manager(ManagerCapacity),
    Scope {
        scope_id: u128,
        capacity: ScopeCapacityKind,
    },
}

impl From<&'static str> for ResourceLabel {
    fn from(value: &'static str) -> Self {
        Self::Named(value)
    }
}

/// Manager-wide capacity pools.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ManagerCapacity {
    ActiveRoots,
    QueuedRoots,
    AdmissionWaiters,
    UnconsumedOutcomes,
    CleanupConcurrency,
    DiagnosticEvents,
}

/// Per-scope capacity pools.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScopeCapacityKind {
    ChildOperations,
    Resources,
    EffectRecords,
}

/// Capacity failure that identifies the exhausted pool without exposing its
/// internal `qubit-budget` accounting state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CapacityError {
    resource: ResourceLabel,
}

impl fmt::Display for CapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "capacity exhausted for {:?}", self.resource)
    }
}

impl Error for CapacityError {}

struct Waiter {
    id: u64,
    waker: Waker,
}

struct CoordinatorState {
    generation: u64,
    waiters: Vec<Option<Waiter>>,
}

/// Coordinates wakeups after RAII permits return to their pools.
pub(crate) struct AdmissionCoordinator {
    transaction: Mutex<()>,
    state: Mutex<CoordinatorState>,
}

impl AdmissionCoordinator {
    pub(crate) fn new(waiter_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            transaction: Mutex::new(()),
            state: Mutex::new(CoordinatorState {
                generation: 0,
                waiters: (0..waiter_limit).map(|_| None).collect(),
            }),
        })
    }

    fn generation(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .generation
    }

    fn register(&self, id: u64, observed_generation: u64, waker: &Waker) {
        let replacement = waker.clone();
        let (old_waker, should_wake) = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.generation != observed_generation {
                (Some(replacement), true)
            } else if let Some(waiter) = state
                .waiters
                .iter_mut()
                .flatten()
                .find(|item| item.id == id)
            {
                if waiter.waker.will_wake(waker) {
                    (Some(replacement), false)
                } else {
                    (
                        Some(std::mem::replace(&mut waiter.waker, replacement)),
                        false,
                    )
                }
            } else if let Some(slot) = state.waiters.iter_mut().find(|slot| slot.is_none()) {
                *slot = Some(Waiter {
                    id,
                    waker: replacement,
                });
                (None, false)
            } else {
                // The waiter permit pool bounds this list. If that invariant is
                // broken, waking makes progress without storing an unbounded
                // registration.
                (Some(replacement), true)
            }
        };
        drop(old_waker);
        if should_wake {
            waker.wake_by_ref();
        }
    }

    fn unregister(&self, id: u64) {
        let removed = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state
                .waiters
                .iter_mut()
                .find(|slot| slot.as_ref().is_some_and(|waiter| waiter.id == id))
                .and_then(Option::take)
        };
        drop(removed);
    }

    fn notify(&self) {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.generation = state.generation.wrapping_add(1);
        }
        loop {
            let waiter = {
                let mut state = self
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                state.waiters.iter_mut().find_map(Option::take)
            };
            let Some(waiter) = waiter else {
                break;
            };
            waiter.waker.wake();
        }
    }

    pub(crate) fn wake_waiters(&self) {
        self.notify();
    }
}

fn default_coordinator() -> Arc<AdmissionCoordinator> {
    static COORDINATOR: OnceLock<Arc<AdmissionCoordinator>> = OnceLock::new();
    Arc::clone(COORDINATOR.get_or_init(|| AdmissionCoordinator::new(4096)))
}

struct PoolInner {
    id: u64,
    budget: ManagedResourcePool<ResourceLabel, usize>,
    coordinator: Arc<AdmissionCoordinator>,
}

/// Cloneable pool handle; clones share the single underlying `rs-budget` pool.
#[derive(Clone)]
pub(crate) struct CapacityPool {
    inner: Arc<PoolInner>,
}

impl CapacityPool {
    pub(crate) fn new(label: impl Into<ResourceLabel>, limit: usize) -> Self {
        Self::with_coordinator(label, limit, default_coordinator())
    }

    fn with_coordinator(
        label: impl Into<ResourceLabel>,
        limit: usize,
        coordinator: Arc<AdmissionCoordinator>,
    ) -> Self {
        Self {
            inner: Arc::new(PoolInner {
                id: NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed),
                budget: ManagedResourcePool::new(label.into(), limit),
                coordinator,
            }),
        }
    }

    pub(crate) fn try_acquire(&self) -> Result<CapacityPermit, CapacityError> {
        self.inner
            .budget
            .try_acquire(1)
            .map(|permit| CapacityPermit {
                permit: Some(permit),
                coordinator: Arc::clone(&self.inner.coordinator),
                pool_id: self.inner.id,
                notify_on_drop: true,
            })
            .map_err(|_| CapacityError {
                resource: self.inner.budget.resource().clone(),
            })
    }

    pub(crate) fn available(&self) -> usize {
        self.inner.budget.available()
    }

    fn id(&self) -> u64 {
        self.inner.id
    }
}

/// One capacity unit returned to the pool and coordinator on drop.
pub(crate) struct CapacityPermit {
    permit: Option<ManagedResourcePermit<ResourceLabel, usize>>,
    coordinator: Arc<AdmissionCoordinator>,
    pool_id: u64,
    notify_on_drop: bool,
}

impl CapacityPermit {
    fn suppress_notification(&mut self) {
        self.notify_on_drop = false;
    }
}

impl Drop for CapacityPermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        if self.notify_on_drop {
            let transaction = self
                .coordinator
                .transaction
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            drop(transaction);
            self.coordinator.notify();
        }
    }
}

/// A rollback-safe collection of permits acquired in a canonical pool order.
pub(crate) struct PermitBundle {
    permits: Vec<CapacityPermit>,
    coordinator: Option<Arc<AdmissionCoordinator>>,
}

impl PermitBundle {
    pub(crate) fn try_acquire(pools: &[CapacityPool]) -> Result<Self, CapacityError> {
        let mut ordered: Vec<_> = pools.iter().collect();
        ordered.sort_unstable_by_key(|pool| pool.id());
        ordered.dedup_by_key(|pool| pool.id());

        let coordinator = ordered
            .first()
            .map(|pool| Arc::clone(&pool.inner.coordinator));
        if let Some(coordinator) = &coordinator {
            if ordered
                .iter()
                .any(|pool| !Arc::ptr_eq(coordinator, &pool.inner.coordinator))
            {
                return Err(CapacityError {
                    resource: ordered[0].inner.budget.resource().clone(),
                });
            }
        }
        let transaction = coordinator.as_ref().map(|coordinator| {
            coordinator
                .transaction
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
        });
        let mut permits = Vec::with_capacity(ordered.len());
        for pool in ordered {
            match pool.try_acquire() {
                Ok(mut permit) => {
                    permit.suppress_notification();
                    permits.push(permit);
                }
                Err(error) => {
                    drop(permits);
                    drop(transaction);
                    return Err(error);
                }
            }
        }
        drop(transaction);
        for permit in &mut permits {
            permit.notify_on_drop = true;
        }
        Ok(Self {
            permits,
            coordinator,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.permits.len()
    }

    /// Moves one pool's permit to a lifecycle-specific owner.
    pub(crate) fn take(&mut self, pool: &CapacityPool) -> Option<CapacityPermit> {
        self.permits
            .iter()
            .position(|permit| permit.pool_id == pool.id())
            .map(|index| self.permits.swap_remove(index))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.permits.is_empty()
    }
}

impl Drop for PermitBundle {
    fn drop(&mut self) {
        let had_permits = !self.permits.is_empty();
        for permit in &mut self.permits {
            permit.suppress_notification();
        }
        self.permits.clear();
        if had_permits {
            if let Some(coordinator) = &self.coordinator {
                let transaction = coordinator
                    .transaction
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                drop(transaction);
                coordinator.notify();
            }
        }
    }
}

/// A cancellable admission future. It never holds a partial bundle while
/// pending; only the bounded waiter registration remains reserved.
pub(crate) struct AdmissionRequest {
    pools: Vec<CapacityPool>,
    coordinator: Arc<AdmissionCoordinator>,
    waiter_id: u64,
    waiter_permit: Option<CapacityPermit>,
    registered: bool,
}

impl AdmissionRequest {
    pub(crate) fn new(
        pools: Vec<CapacityPool>,
        waiter_pool: Option<CapacityPool>,
    ) -> Result<Self, CapacityError> {
        let mut pools = pools;
        pools.sort_unstable_by_key(CapacityPool::id);
        pools.dedup_by_key(|pool| pool.id());
        let coordinator = pools
            .first()
            .map(|pool| Arc::clone(&pool.inner.coordinator))
            .or_else(|| {
                waiter_pool
                    .as_ref()
                    .map(|pool| Arc::clone(&pool.inner.coordinator))
            })
            .unwrap_or_else(default_coordinator);
        if pools
            .iter()
            .any(|pool| !Arc::ptr_eq(&coordinator, &pool.inner.coordinator))
            || waiter_pool
                .as_ref()
                .is_some_and(|pool| !Arc::ptr_eq(&coordinator, &pool.inner.coordinator))
        {
            let resource = waiter_pool
                .as_ref()
                .map(|pool| pool.inner.budget.resource().clone())
                .or_else(|| {
                    pools
                        .first()
                        .map(|pool| pool.inner.budget.resource().clone())
                })
                .unwrap_or(ResourceLabel::Named("capacity coordinator"));
            return Err(CapacityError { resource });
        }
        let waiter_permit = waiter_pool.map(|pool| pool.try_acquire()).transpose()?;
        Ok(Self {
            pools,
            coordinator,
            waiter_id: NEXT_WAITER_ID.fetch_add(1, Ordering::Relaxed),
            waiter_permit,
            registered: false,
        })
    }
}

impl Future for AdmissionRequest {
    type Output = Result<PermitBundle, CapacityError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.registered {
            self.coordinator.unregister(self.waiter_id);
            self.registered = false;
        }
        let observed_generation = self.coordinator.generation();
        match PermitBundle::try_acquire(&self.pools) {
            Ok(bundle) => {
                self.coordinator.unregister(self.waiter_id);
                self.registered = false;
                drop(self.waiter_permit.take());
                Poll::Ready(Ok(bundle))
            }
            Err(_) => {
                self.coordinator
                    .register(self.waiter_id, observed_generation, cx.waker());
                self.registered = true;
                match PermitBundle::try_acquire(&self.pools) {
                    Ok(bundle) => {
                        self.coordinator.unregister(self.waiter_id);
                        self.registered = false;
                        drop(self.waiter_permit.take());
                        Poll::Ready(Ok(bundle))
                    }
                    Err(_) => Poll::Pending,
                }
            }
        }
    }
}

impl Drop for AdmissionRequest {
    fn drop(&mut self) {
        if self.registered {
            self.coordinator.unregister(self.waiter_id);
        }
        drop(self.waiter_permit.take());
    }
}

/// Manager-wide bounded capacities. Each field is one shared rs-budget pool.
pub(crate) struct ManagerCapacityPools {
    active_roots: CapacityPool,
    queued_roots: CapacityPool,
    admission_waiters: CapacityPool,
    unconsumed_outcomes: CapacityPool,
    cleanup_concurrency: CapacityPool,
    diagnostic_events: CapacityPool,
    coordinator: Arc<AdmissionCoordinator>,
}

impl ManagerCapacityPools {
    pub(crate) fn new(limits: ManagerLimits) -> Self {
        let coordinator = AdmissionCoordinator::new(limits.admission_waiters);
        let pool = |capacity, label| {
            CapacityPool::with_coordinator(
                ResourceLabel::Manager(label),
                capacity,
                Arc::clone(&coordinator),
            )
        };
        Self {
            active_roots: pool(limits.active_operations, ManagerCapacity::ActiveRoots),
            queued_roots: pool(limits.queued_operations, ManagerCapacity::QueuedRoots),
            admission_waiters: pool(limits.admission_waiters, ManagerCapacity::AdmissionWaiters),
            unconsumed_outcomes: pool(
                limits.unconsumed_outcomes,
                ManagerCapacity::UnconsumedOutcomes,
            ),
            cleanup_concurrency: pool(
                limits.cleanup_concurrency,
                ManagerCapacity::CleanupConcurrency,
            ),
            diagnostic_events: pool(limits.diagnostic_events, ManagerCapacity::DiagnosticEvents),
            coordinator,
        }
    }

    pub(crate) fn root_admission_permits(&self) -> Result<PermitBundle, CapacityError> {
        PermitBundle::try_acquire(&[self.active_roots.clone(), self.unconsumed_outcomes.clone()])
    }

    /// Reserves root execution and outcome capacity atomically, but returns
    /// the permits separately so their lifecycle owners can release them at
    /// different operation facts.
    pub(crate) fn root_lifecycle_permits(
        &self,
    ) -> Result<(CapacityPermit, CapacityPermit), CapacityError> {
        let mut permits = PermitBundle::try_acquire(&[
            self.active_roots.clone(),
            self.unconsumed_outcomes.clone(),
        ])?;
        Ok((
            permits
                .take(&self.active_roots)
                .expect("active root permit"),
            permits
                .take(&self.unconsumed_outcomes)
                .expect("outcome permit"),
        ))
    }

    pub(crate) fn outcome_permit(&self) -> Result<CapacityPermit, CapacityError> {
        self.unconsumed_outcomes.try_acquire()
    }

    #[cfg(test)]
    fn active_root_permit(&self) -> Result<CapacityPermit, CapacityError> {
        self.active_roots.try_acquire()
    }

    pub(crate) fn root_pools(&self) -> Vec<CapacityPool> {
        vec![self.active_roots.clone(), self.unconsumed_outcomes.clone()]
    }

    pub(crate) fn root_admission_request(&self) -> Result<AdmissionRequest, CapacityError> {
        self.admission_request(vec![
            self.active_roots.clone(),
            self.unconsumed_outcomes.clone(),
        ])
    }

    pub(crate) fn admission_request(
        &self,
        pools: Vec<CapacityPool>,
    ) -> Result<AdmissionRequest, CapacityError> {
        AdmissionRequest::new(pools, Some(self.admission_waiters.clone()))
    }

    pub(crate) fn child_admission_request(
        &self,
        scope: &ScopeCapacity,
    ) -> Result<AdmissionRequest, CapacityError> {
        let mut pools = scope.child_operation_pools.clone();
        pools.push(self.unconsumed_outcomes.clone());
        self.admission_request(pools)
    }

    pub(crate) fn child_lifecycle_permits(
        &self,
        scope: &ScopeCapacity,
    ) -> Result<(PermitBundle, CapacityPermit), CapacityError> {
        let mut permits = PermitBundle::try_acquire(
            &[
                scope.child_operation_pools.as_slice(),
                std::slice::from_ref(&self.unconsumed_outcomes),
            ]
            .concat(),
        )?;
        let outcome = permits
            .take(&self.unconsumed_outcomes)
            .expect("outcome permit");
        Ok((permits, outcome))
    }

    pub(crate) fn child_admission_pools(&self, scope: &ScopeCapacity) -> Vec<CapacityPool> {
        let mut pools = scope.child_operation_pools.clone();
        pools.push(self.unconsumed_outcomes.clone());
        pools
    }

    pub(crate) fn queue_permit(&self) -> Result<CapacityPermit, CapacityError> {
        self.queued_roots.try_acquire()
    }

    pub(crate) fn cleanup_permit(&self) -> Result<CapacityPermit, CapacityError> {
        self.cleanup_concurrency.try_acquire()
    }

    pub(crate) fn diagnostic_permit(&self) -> Result<CapacityPermit, CapacityError> {
        self.diagnostic_events.try_acquire()
    }

    pub(crate) fn coordinator(&self) -> Arc<AdmissionCoordinator> {
        Arc::clone(&self.coordinator)
    }

    pub(crate) fn notify_waiters(&self) {
        self.coordinator.wake_waiters();
    }
}

/// The active capacity path for one operation, including all applicable
/// ancestors. Child scopes add local pools without pre-reserving their limits.
#[derive(Clone)]
pub(crate) struct ScopeCapacity {
    limits: ScopeLimits,
    child_operation_pools: Vec<CapacityPool>,
    resource_pools: Vec<CapacityPool>,
    effect_record_pools: Vec<CapacityPool>,
}

impl ScopeCapacity {
    pub(crate) fn root(manager: &ManagerCapacityPools, scope_id: u64, limits: ScopeLimits) -> Self {
        let coordinator = manager.coordinator();
        let pool = |capacity, kind| {
            CapacityPool::with_coordinator(
                ResourceLabel::Scope {
                    scope_id: u128::from(scope_id),
                    capacity: kind,
                },
                capacity,
                Arc::clone(&coordinator),
            )
        };
        Self {
            limits,
            child_operation_pools: vec![pool(
                limits.child_operations,
                ScopeCapacityKind::ChildOperations,
            )],
            resource_pools: vec![pool(limits.resources, ScopeCapacityKind::Resources)],
            effect_record_pools: vec![pool(
                limits.effect_records,
                ScopeCapacityKind::EffectRecords,
            )],
        }
    }

    pub(crate) fn root_for_operation(
        manager: &ManagerCapacityPools,
        scope_id: crate::v02::report::OperationId,
        limits: ScopeLimits,
    ) -> Self {
        Self::root_with_identity(manager, scope_id.raw(), limits)
    }

    pub(crate) fn root_with_identity(
        manager: &ManagerCapacityPools,
        scope_id: u128,
        limits: ScopeLimits,
    ) -> Self {
        let coordinator = manager.coordinator();
        let pool = |capacity, kind| {
            CapacityPool::with_coordinator(
                ResourceLabel::Scope {
                    scope_id,
                    capacity: kind,
                },
                capacity,
                Arc::clone(&coordinator),
            )
        };
        Self {
            limits,
            child_operation_pools: vec![pool(
                limits.child_operations,
                ScopeCapacityKind::ChildOperations,
            )],
            resource_pools: vec![pool(limits.resources, ScopeCapacityKind::Resources)],
            effect_record_pools: vec![pool(
                limits.effect_records,
                ScopeCapacityKind::EffectRecords,
            )],
        }
    }

    pub(crate) fn child(&self, scope_id: u64, requested: ScopeLimits) -> Self {
        self.child_with_identity(u128::from(scope_id), requested)
    }

    pub(crate) fn child_for_operation(
        &self,
        scope_id: crate::v02::report::OperationId,
        requested: ScopeLimits,
    ) -> Self {
        self.child_with_identity(scope_id.raw(), requested)
    }

    fn child_with_identity(&self, scope_id: u128, requested: ScopeLimits) -> Self {
        let limits = ScopeLimits {
            child_operations: self.limits.child_operations.min(requested.child_operations),
            resources: self.limits.resources.min(requested.resources),
            effect_records: self.limits.effect_records.min(requested.effect_records),
        };
        let coordinator = Arc::clone(&self.child_operation_pools[0].inner.coordinator);
        let pool = |capacity, kind| {
            CapacityPool::with_coordinator(
                ResourceLabel::Scope {
                    scope_id,
                    capacity: kind,
                },
                capacity,
                Arc::clone(&coordinator),
            )
        };
        let mut child_operation_pools = self.child_operation_pools.clone();
        child_operation_pools.push(pool(
            limits.child_operations,
            ScopeCapacityKind::ChildOperations,
        ));
        let mut resource_pools = self.resource_pools.clone();
        resource_pools.push(pool(limits.resources, ScopeCapacityKind::Resources));
        let mut effect_record_pools = self.effect_record_pools.clone();
        effect_record_pools.push(pool(
            limits.effect_records,
            ScopeCapacityKind::EffectRecords,
        ));
        Self {
            limits,
            child_operation_pools,
            resource_pools,
            effect_record_pools,
        }
    }

    pub(crate) fn effective_limits(&self) -> ScopeLimits {
        self.limits
    }

    pub(crate) fn child_operation_permits(&self) -> Result<PermitBundle, CapacityError> {
        PermitBundle::try_acquire(&self.child_operation_pools)
    }

    pub(crate) fn child_operation_pool_plan(&self) -> Vec<CapacityPool> {
        self.child_operation_pools.clone()
    }

    pub(crate) fn resource_registration(&self) -> Result<ResourceRegistration, CapacityError> {
        PermitBundle::try_acquire(&self.resource_pools).map(|permits| ResourceRegistration {
            permits: Arc::new(permits),
        })
    }

    pub(crate) fn effect_record_permits(&self) -> Result<PermitBundle, CapacityError> {
        PermitBundle::try_acquire(&self.effect_record_pools)
    }

    pub(crate) fn child_capacity_available(&self) -> usize {
        self.child_operation_pools
            .iter()
            .map(CapacityPool::available)
            .min()
            .unwrap_or(0)
    }

    pub(crate) fn resource_capacity_available(&self) -> usize {
        self.resource_pools
            .iter()
            .map(CapacityPool::available)
            .min()
            .unwrap_or(0)
    }
}

/// A single registration shared by clones of a resource reference.
#[derive(Clone)]
pub(crate) struct ResourceRegistration {
    permits: Arc<PermitBundle>,
}

impl ResourceRegistration {
    pub(crate) fn permit_count(&self) -> usize {
        self.permits.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdmissionRequest, CapacityPool, ManagerCapacityPools, PermitBundle, ResourceRegistration,
        ScopeCapacity,
    };
    use crate::v02::report::{ManagerLimits, ScopeLimits};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll, Wake, Waker};

    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn poll<F: Future + Unpin>(future: &mut F, waker: &Waker) -> Poll<F::Output> {
        Pin::new(future).poll(&mut Context::from_waker(waker))
    }

    #[test]
    fn pool_clone_does_not_duplicate_capacity() {
        let pool = CapacityPool::new("test", 1);
        let clone = pool.clone();
        let permit = pool.try_acquire().expect("first pool handle acquires");

        assert!(clone.try_acquire().is_err());
        drop(permit);
        assert_eq!(clone.available(), 1);
    }

    #[test]
    fn failed_bundle_rolls_back_all_pools() {
        let first = CapacityPool::new("first", 1);
        let second = CapacityPool::new("second", 1);
        let held = second.try_acquire().expect("occupy second pool");

        assert!(PermitBundle::try_acquire(&[first.clone(), second.clone()]).is_err());
        assert_eq!(first.available(), 1);
        assert_eq!(second.available(), 0);

        drop(held);
    }

    #[test]
    fn root_permits_can_follow_outcome_and_settlement_separately() {
        let manager = ManagerCapacityPools::new(ManagerLimits {
            active_operations: 1,
            unconsumed_outcomes: 1,
            ..ManagerLimits::default()
        });
        let mut bundle = manager
            .root_admission_permits()
            .expect("root and outcome capacities are available");
        let active = bundle
            .take(&manager.active_roots)
            .expect("root permit is separately transferable");
        let outcome = bundle
            .take(&manager.unconsumed_outcomes)
            .expect("outcome permit is separately transferable");

        drop(active);
        assert_eq!(manager.active_roots.available(), 1);
        assert_eq!(manager.unconsumed_outcomes.available(), 0);
        drop(outcome);
        assert_eq!(manager.unconsumed_outcomes.available(), 1);
    }

    #[test]
    fn grandchild_uses_root_and_local_pools() {
        let manager = ManagerCapacityPools::new(ManagerLimits::default());
        let root = ScopeCapacity::root(
            &manager,
            1,
            ScopeLimits {
                child_operations: 2,
                resources: 3,
                effect_records: 4,
            },
        );
        let child = root.child(
            2,
            ScopeLimits {
                child_operations: 1,
                resources: 8,
                effect_records: 2,
            },
        );
        let grandchild = child.child(3, ScopeLimits::default());

        assert_eq!(root.effective_limits().child_operations, 2);
        assert_eq!(child.effective_limits().resources, 3);
        assert_eq!(grandchild.effective_limits().effect_records, 2);
        assert_eq!(root.child_capacity_available(), 2);
        let first = grandchild
            .child_operation_permits()
            .expect("first grandchild");
        assert_eq!(root.child_capacity_available(), 1);
        assert_eq!(child.child_capacity_available(), 0);
        assert!(grandchild.child_operation_permits().is_err());
        drop(first);
        assert_eq!(root.child_capacity_available(), 2);
        assert_eq!(child.child_capacity_available(), 1);
    }

    #[test]
    fn resource_ref_clone_is_not_registration() {
        let manager = ManagerCapacityPools::new(ManagerLimits::default());
        let scope = ScopeCapacity::root(&manager, 10, ScopeLimits::default());
        let registration: ResourceRegistration = scope
            .resource_registration()
            .expect("resource registration succeeds");
        let clone = registration.clone();
        assert_eq!(clone.permit_count(), 1);

        assert_eq!(
            scope.resource_capacity_available(),
            ScopeLimits::default().resources - 1
        );
        drop(registration);
        assert_eq!(
            scope.resource_capacity_available(),
            ScopeLimits::default().resources - 1
        );
        drop(clone);
        assert_eq!(
            scope.resource_capacity_available(),
            ScopeLimits::default().resources
        );
    }

    #[test]
    fn release_notifies_registered_waiter() {
        let pool = CapacityPool::new("one", 1);
        let held = pool.try_acquire().expect("hold the only slot");
        let mut waiting = AdmissionRequest::new(vec![pool.clone()], None)
            .expect("waiter registration has capacity");
        let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wakes));

        assert!(poll(&mut waiting, &waker).is_pending());
        drop(held);
        assert!(wakes.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(poll(&mut waiting, &waker), Poll::Ready(Ok(_))));
    }

    #[test]
    fn cancelled_admission_returns_every_permit() {
        let first = CapacityPool::new("first", 1);
        let second = CapacityPool::new("second", 1);
        let waiters = CapacityPool::new("waiter", 1);
        let held_second = second.try_acquire().expect("hold second pool");
        let mut waiting =
            AdmissionRequest::new(vec![first.clone(), second.clone()], Some(waiters.clone()))
                .expect("waiter slot is available");
        let waker = Waker::from(Arc::new(WakeCount(AtomicUsize::new(0))));

        assert!(poll(&mut waiting, &waker).is_pending());
        assert_eq!(first.available(), 1);
        assert_eq!(waiters.available(), 0);
        drop(waiting);

        assert_eq!(first.available(), 1);
        assert_eq!(second.available(), 0);
        assert_eq!(waiters.available(), 1);
        drop(held_second);
    }

    #[test]
    fn root_execution_and_outcome_permits_have_distinct_lifetimes() {
        let manager = ManagerCapacityPools::new(ManagerLimits {
            active_operations: 1,
            unconsumed_outcomes: 1,
            ..ManagerLimits::default()
        });
        let (execution, outcome) = manager.root_lifecycle_permits().unwrap();
        drop(execution);
        let next_execution = manager.active_root_permit().unwrap();
        assert!(manager.outcome_permit().is_err());
        drop(outcome);
        assert!(manager.outcome_permit().is_ok());
        drop(next_execution);
    }
}

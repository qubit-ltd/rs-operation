//! Child registration and structured scope composition.

use crate::context::ContextOptions;
use crate::protocol::{ManagedOperationControl, OwnedCleanup};
use crate::report::{
    ChildBusinessOutcome, ChildBusinessSummary, ChildSettlement, OperationId, RecoveryKey,
    RecoveryOwner, ReleaseReceipt, ResourceId, ScopeLimits, StopReason,
};
use crate::resource::{
    CleanupScheduler, RecoveryRegistry, RegisterError, RegistryError, ResourceError,
    ResourceHandle, ResourceRegistry, ResourceUse, TransferError,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

static NEXT_SCOPE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1_000_000);

/// Parent behavior after a required child reports failure.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ScopePolicy {
    #[default]
    FailFast,
    Collect,
}

/// Whether a child failure affects its parent operation result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildRequirement {
    Required,
    Optional,
}

/// Options fixed when a root or child scope is created.
#[derive(Clone, Default)]
pub struct ScopeOptions {
    pub limits: ScopeLimits,
    pub policy: ScopePolicy,
    pub context: ContextOptions,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScopeError {
    Closed,
    LimitReached,
    AlreadyAttached,
    NotAttached,
    Cancelled,
}

/// Aggregate of all committed child operations after business and release observation.
#[derive(Clone, Debug, Default)]
pub struct ScopeJoinReport {
    pub settlements: Vec<ChildSettlement>,
    pub required_failures: Vec<ChildBusinessSummary>,
}

struct Child {
    id: OperationId,
    control: Arc<dyn ManagedOperationControl>,
    requirement: ChildRequirement,
    committed: bool,
    uses: Option<Vec<ResourceUse>>,
}
struct State {
    accepting: bool,
    stop: Option<StopReason>,
    children: Vec<Child>,
    child_scopes: Vec<OperationScope>,
}

/// A scope that owns child registration authority and waits for registered work.
#[derive(Clone)]
pub struct OperationScope {
    state: Arc<Mutex<State>>,
    limits: ScopeLimits,
    policy: ScopePolicy,
    resources: ResourceRegistry,
    handles: Arc<Mutex<BTreeMap<ResourceId, ResourceHandle>>>,
    cleanup: CleanupScheduler,
    recovery: RecoveryRegistry,
    recovery_keys: Arc<Mutex<Vec<RecoveryKey<RecoveryOwner>>>>,
    resource_wakers: Arc<Mutex<Vec<std::task::Waker>>>,
    resource_epoch: Arc<std::sync::atomic::AtomicU64>,
}

impl OperationScope {
    pub(crate) fn new(
        operation: OperationId,
        limits: ScopeLimits,
        policy: ScopePolicy,
        cleanup: CleanupScheduler,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                accepting: true,
                stop: None,
                children: Vec::new(),
                child_scopes: Vec::new(),
            })),
            limits,
            policy,
            resources: ResourceRegistry::new(operation, limits.resources),
            handles: Arc::default(),
            cleanup,
            recovery: RecoveryRegistry::new(limits.resources),
            recovery_keys: Arc::default(),
            resource_wakers: Arc::default(),
            resource_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// Create an owned child scope whose accepted work and resources join this scope.
    pub fn child_scope(&self, options: ScopeOptions) -> Result<OperationScope, ScopeError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.accepting {
            return Err(ScopeError::Closed);
        }
        if state.child_scopes.len() >= self.limits.child_operations {
            return Err(ScopeError::LimitReached);
        }
        if options.limits.child_operations > self.limits.child_operations
            || options.limits.resources > self.limits.resources
        {
            return Err(ScopeError::LimitReached);
        }
        let child_id = OperationId::new(
            NEXT_SCOPE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u128,
        );
        let child = OperationScope::new(
            child_id,
            options.limits,
            options.policy,
            self.cleanup.clone(),
        );
        if let Some(reason) = state.stop {
            child.request_cancel(reason);
        }
        state.child_scopes.push(child.clone());
        Ok(child)
    }

    /// Register a cleanup owner before creating or exposing its resource.
    pub fn register_resource<C: OwnedCleanup>(
        &self,
        owner: C,
    ) -> Result<ResourceId, RegisterError> {
        let handle = self.resources.register(Box::new(owner))?;
        let id = handle.id();
        self.handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, handle);
        Ok(id)
    }

    /// Acquire one non-clone use authorization for a registered resource.
    pub fn acquire_use(&self, id: ResourceId) -> Result<ResourceUse, ResourceError> {
        self.handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .ok_or(ResourceError::NotRegistered)?
            .acquire_use()
    }

    /// Close future use and make the resource eligible for cleanup after its leases return.
    pub fn close_for_use(&self, id: ResourceId) -> Result<(), ResourceError> {
        self.handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .ok_or(ResourceError::NotRegistered)?
            .close_for_use()
    }

    /// Order one resource's cleanup after a prerequisite resource is released.
    pub fn cleanup_after(
        &self,
        resource: ResourceId,
        prerequisite: ResourceId,
    ) -> Result<(), crate::resource::DependencyError> {
        let handles = self.handles.lock().unwrap_or_else(|e| e.into_inner());
        handles
            .get(&resource)
            .ok_or(crate::resource::DependencyError::NotRegistered)?
            .cleanup_after(
                handles
                    .get(&prerequisite)
                    .ok_or(crate::resource::DependencyError::NotRegistered)?,
            )
    }

    /// Transfer a resource and its connected dependency group to another scope atomically.
    pub fn transfer_resource_to(
        &self,
        id: ResourceId,
        destination: &OperationScope,
    ) -> Result<ResourceId, TransferError> {
        let mut handles = self.handles.lock().unwrap_or_else(|e| e.into_inner());
        let handle = handles.get(&id).ok_or(TransferError::NotRegistered)?;
        self.resources.transfer_to(handle, &destination.resources)?;
        let new_id = handle.id();
        let handle = handles.remove(&id).expect("handle checked");
        destination
            .handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(new_id, handle);
        Ok(new_id)
    }

    /// Take a one-use typed recovery key produced by cleanup settlement.
    pub fn take_recovery_key(&self) -> Option<RecoveryKey<RecoveryOwner>> {
        self.recovery_keys
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .pop()
    }

    /// Recover an owner only with its original registry-issued key.
    pub fn take_recovery<R: Send + 'static>(
        &self,
        key: RecoveryKey<R>,
    ) -> Result<R, RecoveryKey<R>> {
        self.recovery.take_recovery(key)
    }

    /// Confirm release of a transferred owner using the receiver's release proof.
    pub fn confirm_transferred_release(
        &self,
        id: ResourceId,
        receipt: ReleaseReceipt,
    ) -> Result<(), RegistryError> {
        self.resources.confirm_transferred_released(id, receipt)?;
        self.handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
        self.resource_epoch
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let waiters = std::mem::take(
            &mut *self
                .resource_wakers
                .lock()
                .unwrap_or_else(|e| e.into_inner()),
        );
        for waker in waiters {
            waker.wake();
        }
        Ok(())
    }

    /// Check whether this scope still owns a registration or source tombstone.
    pub fn contains_resource(&self, id: ResourceId) -> bool {
        self.resources.contains(id)
    }

    pub(crate) fn close_body_resources(&self) {
        let handles = self.handles.lock().unwrap_or_else(|e| e.into_inner());
        for handle in handles.values() {
            let _ = handle.close_for_use();
        }
        let _ = (&self.cleanup, &self.resources, &self.recovery);
    }

    pub(crate) fn resource_registry(&self) -> ResourceRegistry {
        self.resources.clone()
    }
    pub(crate) fn cleanup_scheduler(&self) -> CleanupScheduler {
        self.cleanup.clone()
    }
    pub(crate) fn recovery_registry(&self) -> RecoveryRegistry {
        self.recovery.clone()
    }
    pub(crate) fn save_recovery_key(&self, key: RecoveryKey<RecoveryOwner>) {
        self.recovery_keys
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(key);
    }
    pub(crate) fn resource_epoch(&self) -> u64 {
        self.resource_epoch
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) async fn wait_for_resource_change(&self, seen: u64) {
        std::future::poll_fn(|cx| {
            if self.resource_epoch() != seen
                || self.resources.has_ready_cleanup()
                || self.resources.pending_cleanup_count() == 0
            {
                return Poll::Ready(());
            }
            let mut waiters = self
                .resource_wakers
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !waiters.iter().any(|w| w.will_wake(cx.waker())) {
                waiters.push(cx.waker().clone());
            }
            if self.resource_epoch() != seen
                || self.resources.has_ready_cleanup()
                || self.resources.pending_cleanup_count() == 0
            {
                waiters.retain(|w| !w.will_wake(cx.waker()));
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// Reserve a child registration slot before handing work to a domain executor.
    pub fn reserve_child(
        &self,
        requirement: ChildRequirement,
    ) -> Result<ChildReservation, ScopeError> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.accepting {
            return Err(ScopeError::Closed);
        }
        if state.children.len() >= self.limits.child_operations {
            return Err(ScopeError::LimitReached);
        }
        Ok(ChildReservation {
            scope: Arc::clone(&self.state),
            requirement,
            control: None,
            uses: Vec::new(),
            committed: false,
        })
    }

    /// Request cancellation of this scope and all children already committed.
    pub fn request_cancel(&self, reason: StopReason) {
        let (controls, scopes) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.stop.get_or_insert(reason);
            (
                state
                    .children
                    .iter()
                    .filter(|child| child.committed)
                    .map(|child| Arc::clone(&child.control))
                    .collect::<Vec<_>>(),
                state.child_scopes.clone(),
            )
        };
        for control in controls {
            control.request_stop(reason);
        }
        for scope in scopes {
            scope.request_cancel(reason);
        }
    }

    /// Close this body’s authority to create more child registrations.
    pub(crate) fn close_body(&self) {
        let children = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.accepting = false;
            state.child_scopes.clone()
        };
        for child in children {
            child.close_body();
            child.close_body_resources();
        }
    }

    pub(crate) fn child_scopes(&self) -> Vec<OperationScope> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .child_scopes
            .clone()
    }

    /// The immutable policy selected at scope creation.
    pub fn policy(&self) -> ScopePolicy {
        self.policy
    }

    /// Close this scope's body submissions and wait for every committed child to settle.
    pub async fn join(&self) -> ScopeJoinReport {
        self.close_body();
        self.close_body_resources();
        let mut facts = JoinFacts::default();
        std::future::poll_fn(|cx| poll_scope_business(self, cx, &mut facts)).await;
        std::future::poll_fn(|cx| poll_scope_releases(self, cx, &mut facts)).await;
        let _settlement = crate::operation::cleanup_scope_tree(self).await;
        ScopeJoinReport {
            settlements: facts.settlements,
            required_failures: facts.failed_required.into_iter().collect(),
        }
    }
}

/// A two-phase registration reservation. Dropping before commit has no child side effect.
pub struct ChildReservation {
    scope: Arc<Mutex<State>>,
    requirement: ChildRequirement,
    control: Option<Arc<dyn ManagedOperationControl>>,
    uses: Vec<ResourceUse>,
    committed: bool,
}

impl ChildReservation {
    /// Attach a suspended domain operation control before it may be started.
    pub fn attach(&mut self, control: Arc<dyn ManagedOperationControl>) -> Result<(), ScopeError> {
        if self.control.is_some() {
            return Err(ScopeError::AlreadyAttached);
        }
        self.control = Some(control);
        Ok(())
    }

    /// Attach a child control and transfer any resource-use leases it requires.
    pub fn attach_with_uses(
        &mut self,
        control: Arc<dyn ManagedOperationControl>,
        uses: Vec<ResourceUse>,
    ) -> Result<(), ScopeError> {
        self.attach(control)?;
        self.uses = uses;
        Ok(())
    }

    /// Linearize child start authorization against cancellation.
    pub fn commit(mut self) -> Result<(), ScopeError> {
        let control = self.control.take().ok_or(ScopeError::NotAttached)?;
        let child_id = control.snapshot().operation_id;
        let mut state = self.scope.lock().unwrap_or_else(|e| e.into_inner());
        if state.stop.is_some() {
            return Err(ScopeError::Cancelled);
        }
        if !state.accepting {
            return Err(ScopeError::Closed);
        }
        state.children.push(Child {
            id: child_id,
            control,
            requirement: self.requirement,
            committed: true,
            uses: Some(std::mem::take(&mut self.uses)),
        });
        self.committed = true;
        Ok(())
    }
}

impl Drop for ChildReservation {
    fn drop(&mut self) { /* uncommitted reservations own no external work */
    }
}

/// Business and release facts collected from registered children.
#[derive(Default)]
pub(crate) struct JoinFacts {
    pub failed_required: Option<ChildBusinessSummary>,
    pub business: Vec<ChildBusinessSummary>,
    pub settlements: Vec<ChildSettlement>,
}

pub(crate) fn poll_child_business(
    scope: &OperationScope,
    cx: &mut Context<'_>,
    facts: &mut JoinFacts,
) -> Poll<()> {
    let children = {
        let state = scope.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .children
            .iter()
            .filter(|child| child.committed)
            .map(|child| (Arc::clone(&child.control), child.requirement))
            .collect::<Vec<_>>()
    };
    let mut pending = false;
    for (control, requirement) in children {
        let id = control.snapshot().operation_id;
        if facts
            .business
            .iter()
            .any(|summary| summary.operation_id == id)
        {
            continue;
        }
        if let Poll::Ready(summary) = control.poll_business(cx) {
            if requirement == ChildRequirement::Required
                && summary.outcome != ChildBusinessOutcome::Succeeded
                && facts.failed_required.is_none()
            {
                facts.failed_required = Some(summary.clone());
                if scope.policy == ScopePolicy::FailFast {
                    scope.request_cancel(StopReason::RequiredChildFailed);
                }
            }
            facts.business.push(summary);
        } else {
            pending = true;
        }
    }
    if pending {
        Poll::Pending
    } else {
        Poll::Ready(())
    }
}

pub(crate) fn poll_child_releases(
    scope: &OperationScope,
    cx: &mut Context<'_>,
    facts: &mut JoinFacts,
) -> Poll<()> {
    let children = {
        let state = scope.state.lock().unwrap_or_else(|e| e.into_inner());
        state
            .children
            .iter()
            .filter(|child| child.committed)
            .map(|child| (child.id, Arc::clone(&child.control)))
            .collect::<Vec<_>>()
    };
    let mut pending = false;
    for (id, control) in children {
        if facts
            .settlements
            .iter()
            .any(|settlement| settlement.operation_id == id)
        {
            continue;
        }
        match control.poll_released(cx) {
            Poll::Ready(settlement) => {
                facts.settlements.push(settlement);
                let uses = {
                    let mut state = scope.state.lock().unwrap_or_else(|e| e.into_inner());
                    state
                        .children
                        .iter_mut()
                        .find(|child| child.id == id)
                        .and_then(|child| child.uses.take())
                };
                drop(uses);
            }
            Poll::Pending => pending = true,
        }
    }
    if pending {
        Poll::Pending
    } else {
        Poll::Ready(())
    }
}

pub(crate) fn poll_scope_business(
    scope: &OperationScope,
    cx: &mut Context<'_>,
    facts: &mut JoinFacts,
) -> Poll<()> {
    if poll_child_business(scope, cx, facts).is_pending() {
        return Poll::Pending;
    }
    for child in scope.child_scopes() {
        if poll_scope_business(&child, cx, facts).is_pending() {
            return Poll::Pending;
        }
    }
    Poll::Ready(())
}

pub(crate) fn poll_scope_releases(
    scope: &OperationScope,
    cx: &mut Context<'_>,
    facts: &mut JoinFacts,
) -> Poll<()> {
    if poll_child_releases(scope, cx, facts).is_pending() {
        return Poll::Pending;
    }
    for child in scope.child_scopes() {
        if poll_scope_releases(&child, cx, facts).is_pending() {
            return Poll::Pending;
        }
    }
    Poll::Ready(())
}

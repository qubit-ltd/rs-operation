//! Child registration and structured scope composition.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use crate::context::ContextError;
use crate::context::ContextOptions;
use crate::context::DeadlineSignal;
use crate::context::OperationContext;
use crate::protocol::ManagedOperationControl;
use crate::protocol::OwnedCleanup;
use crate::report::ChildBusinessOutcome;
use crate::report::ChildBusinessSummary;
use crate::report::ChildSettlement;
use crate::report::EffectRecord;
use crate::report::OperationId;
use crate::report::RecoveryKey;
use crate::report::RecoveryOwner;
use crate::report::ReleaseReceipt;
use crate::report::ResourceId;
use crate::report::ScopeLimits;
use crate::report::StopReason;
use crate::resource::CleanupScheduler;
use crate::resource::RecoveryRegistry;
use crate::resource::RegisterError;
use crate::resource::RegistryError;
use crate::resource::ResourceError;
use crate::resource::ResourceHandle;
use crate::resource::ResourceRegistry;
use crate::resource::ResourceUse;
use crate::resource::TransferError;

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
    EffectLimitReached,
    AlreadyAttached,
    NotAttached,
    Cancelled,
    InvalidContext,
}

/// Aggregate of all committed child operations after business and release
/// observation.
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
    reserved_children: usize,
}

/// A scope that owns child registration authority and waits for registered
/// work.
#[derive(Clone)]
pub struct OperationScope {
    operation_id: OperationId,
    state: Arc<Mutex<State>>,
    limits: ScopeLimits,
    policy: ScopePolicy,
    resources: ResourceRegistry,
    handles: Arc<Mutex<BTreeMap<ResourceId, ResourceHandle>>>,
    cleanup: CleanupScheduler,
    recovery: RecoveryRegistry,
    recovery_keys: Arc<Mutex<Vec<RecoveryKey<RecoveryOwner>>>>,
    context: OperationContext,
    deadline_signal: Arc<Mutex<Option<DeadlineSignal>>>,
    effects: Arc<Mutex<EffectState>>,
}

#[derive(Default)]
struct EffectState {
    limit: usize,
    records: Vec<EffectRecord>,
    overflow: usize,
}

impl OperationScope {
    pub(crate) fn new(
        operation: OperationId,
        limits: ScopeLimits,
        policy: ScopePolicy,
        cleanup: CleanupScheduler,
        context: OperationContext,
    ) -> Self {
        let deadline_signal = Arc::new(Mutex::new(context.deadline_signal()));
        Self {
            operation_id: operation,
            state: Arc::new(Mutex::new(State {
                accepting: true,
                stop: None,
                children: Vec::new(),
                child_scopes: Vec::new(),
                reserved_children: 0,
            })),
            limits,
            policy,
            resources: ResourceRegistry::new(operation, limits.resources),
            handles: Arc::default(),
            cleanup,
            recovery: RecoveryRegistry::new(limits.resources),
            recovery_keys: Arc::default(),
            context,
            deadline_signal,
            effects: Arc::new(Mutex::new(EffectState {
                limit: limits.effect_records,
                ..EffectState::default()
            })),
        }
    }

    /// Create an owned child scope whose accepted work and resources join this
    /// scope.
    pub fn child_scope(&self, options: ScopeOptions) -> Result<OperationScope, ScopeError> {
        let context = self
            .context
            .child(options.context.clone())
            .map_err(|_error: ContextError| ScopeError::InvalidContext)?;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.accepting {
            return Err(ScopeError::Closed);
        }
        if state.child_scopes.len() >= self.limits.child_operations {
            return Err(ScopeError::LimitReached);
        }
        if options.limits.child_operations > self.limits.child_operations
            || options.limits.resources > self.limits.resources
            || options.limits.effect_records > self.limits.effect_records
        {
            return Err(ScopeError::LimitReached);
        }
        let child_id = OperationId::new(NEXT_SCOPE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u128);
        let mut child = OperationScope::new(child_id, options.limits, options.policy, self.cleanup.clone(), context);
        child.effects = Arc::clone(&self.effects);
        if let Some(reason) = state.stop {
            child.request_cancel(reason);
        }
        state.child_scopes.push(child.clone());
        Ok(child)
    }

    /// Return the identity of this operation scope.
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Retain a caller-declared side-effect fact for the final settlement
    /// report.
    pub fn record_effect(&self, effect: EffectRecord) -> Result<(), ScopeError> {
        let mut state = self.effects.lock().unwrap_or_else(|e| e.into_inner());
        if state.records.len() >= state.limit {
            state.overflow = state.overflow.saturating_add(1);
            return Err(ScopeError::EffectLimitReached);
        }
        state.records.push(effect);
        Ok(())
    }

    pub(crate) fn effect_snapshot(&self) -> (Arc<[EffectRecord]>, usize) {
        let state = self.effects.lock().unwrap_or_else(|e| e.into_inner());
        (Arc::from(state.records.clone()), state.overflow)
    }

    /// Register a cleanup owner before creating or exposing its resource.
    pub fn register_resource<C: OwnedCleanup>(&self, owner: C) -> Result<ResourceId, RegisterError> {
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

    /// Close future use and make the resource eligible for cleanup after its
    /// leases return.
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

    /// Transfer a resource and its connected dependency group to another scope
    /// atomically.
    pub fn transfer_resource_to(
        &self,
        id: ResourceId,
        destination: &OperationScope,
    ) -> Result<ResourceId, TransferError> {
        if Arc::ptr_eq(&self.handles, &destination.handles) {
            return Err(TransferError::SameRegistry);
        }
        let self_first = Arc::as_ptr(&self.handles) < Arc::as_ptr(&destination.handles);
        if self_first {
            let mut source = self.handles.lock().unwrap_or_else(|e| e.into_inner());
            let mut target = destination.handles.lock().unwrap_or_else(|e| e.into_inner());
            self.transfer_with_locked_handles(id, destination, &mut source, &mut target)
        } else {
            let mut target = destination.handles.lock().unwrap_or_else(|e| e.into_inner());
            let mut source = self.handles.lock().unwrap_or_else(|e| e.into_inner());
            self.transfer_with_locked_handles(id, destination, &mut source, &mut target)
        }
    }

    fn transfer_with_locked_handles(
        &self,
        id: ResourceId,
        destination: &OperationScope,
        source: &mut BTreeMap<ResourceId, ResourceHandle>,
        target: &mut BTreeMap<ResourceId, ResourceHandle>,
    ) -> Result<ResourceId, TransferError> {
        let handle = source.get(&id).ok_or(TransferError::NotRegistered)?;
        self.resources.transfer_to(handle, &destination.resources)?;
        let new_id = handle.id();
        let handle = source.remove(&id).expect("source handle was checked");
        target.insert(new_id, handle);
        Ok(new_id)
    }

    /// Take a one-use typed recovery key produced by cleanup settlement.
    pub fn take_recovery_key(&self) -> Option<RecoveryKey<RecoveryOwner>> {
        self.recovery_keys.lock().unwrap_or_else(|e| e.into_inner()).pop()
    }

    /// Recover an owner only with its original registry-issued key.
    pub fn take_recovery<R: Send + 'static>(&self, key: RecoveryKey<R>) -> Result<R, RecoveryKey<R>> {
        self.recovery.take_recovery(key)
    }

    /// Confirm release of a transferred owner using the receiver's release
    /// proof.
    pub fn confirm_transferred_release(&self, id: ResourceId, receipt: ReleaseReceipt) -> Result<(), RegistryError> {
        self.resources.confirm_transferred_released(id, receipt)?;
        self.handles.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
        self.resources.notify_change();
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

    pub(crate) fn poll_deadlines(&self, cx: &mut Context<'_>) {
        let mut signal = self.deadline_signal.lock().unwrap_or_else(|e| e.into_inner()).take();
        if let Some(mut signal) = signal.take() {
            if Pin::new(&mut signal).poll(cx).is_ready() {
                self.request_cancel(StopReason::Timeout);
            } else {
                *self.deadline_signal.lock().unwrap_or_else(|e| e.into_inner()) = Some(signal);
            }
        }
        for child in self.child_scopes() {
            child.poll_deadlines(cx);
        }
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
        self.recovery_keys.lock().unwrap_or_else(|e| e.into_inner()).push(key);
    }
    pub(crate) fn resource_epoch(&self) -> u64 {
        self.resources.change_epoch()
    }

    pub(crate) async fn wait_for_resource_change(&self, seen: u64) {
        std::future::poll_fn(|cx| {
            if self.resource_epoch() != seen
                || self.resources.has_ready_cleanup()
                || self.resources.pending_cleanup_count() == 0
            {
                return Poll::Ready(());
            }
            self.resources.register_change_waker(cx.waker());
            if self.resource_epoch() != seen
                || self.resources.has_ready_cleanup()
                || self.resources.pending_cleanup_count() == 0
            {
                self.resources.remove_change_waker(cx.waker());
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }

    /// Reserve a child registration slot before handing work to a domain
    /// executor.
    pub fn reserve_child(&self, requirement: ChildRequirement) -> Result<ChildReservation, ScopeError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.accepting {
            return Err(ScopeError::Closed);
        }
        if state.children.len() + state.reserved_children >= self.limits.child_operations {
            return Err(ScopeError::LimitReached);
        }
        state.reserved_children += 1;
        Ok(ChildReservation {
            scope: Arc::clone(&self.state),
            requirement,
            control: None,
            uses: Vec::new(),
            committed: false,
            reserved: true,
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

    /// Close this scope's body submissions and wait for every committed child
    /// to settle.
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

/// A two-phase registration reservation. Dropping before commit has no child
/// side effect.
pub struct ChildReservation {
    scope: Arc<Mutex<State>>,
    requirement: ChildRequirement,
    control: Option<Arc<dyn ManagedOperationControl>>,
    uses: Vec<ResourceUse>,
    committed: bool,
    reserved: bool,
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
        state.reserved_children -= 1;
        self.reserved = false;
        self.committed = true;
        Ok(())
    }
}

impl Drop for ChildReservation {
    fn drop(&mut self) {
        if self.reserved {
            let mut state = self.scope.lock().unwrap_or_else(|e| e.into_inner());
            state.reserved_children -= 1;
            self.reserved = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::RawWaker;
    use std::task::RawWakerVTable;
    use std::task::Wake;
    use std::task::Waker;

    use super::OperationScope;
    use super::ScopePolicy;
    use crate::context::ContextOptions;
    use crate::context::OperationContext;
    use crate::protocol::CleanupContext;
    use crate::protocol::OwnedCleanup;
    use crate::report::CleanupOutcome;
    use crate::report::OperationId;
    use crate::report::ScopeLimits;
    use crate::resource::CleanupScheduler;
    use crate::resource::ResourceRegistry;

    struct ReadyCleanup;

    impl OwnedCleanup for ReadyCleanup {
        fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
            Box::pin(std::future::ready(CleanupOutcome::Cleaned))
        }
    }

    struct WakeCount(AtomicUsize);

    impl Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ReentrantProbe {
        registry: ResourceRegistry,
        lock_timeouts: Arc<AtomicUsize>,
    }

    fn check_registry_reentry(probe: &ReentrantProbe) {
        let registry = probe.registry.clone();
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            registry.register_change_waker(Waker::noop());
            let _ = tx.send(());
        });
        if rx.recv_timeout(std::time::Duration::from_millis(100)).is_err() {
            probe.lock_timeouts.fetch_add(1, Ordering::SeqCst);
        }
    }

    unsafe fn clone_reentrant_waker(data: *const ()) -> RawWaker {
        let probe = unsafe { Arc::<ReentrantProbe>::from_raw(data.cast()) };
        check_registry_reentry(&probe);
        let clone = Arc::clone(&probe);
        std::mem::forget(probe);
        RawWaker::new(Arc::into_raw(clone).cast(), &REENTRANT_VTABLE)
    }

    unsafe fn wake_reentrant_waker(data: *const ()) {
        let probe = unsafe { Arc::<ReentrantProbe>::from_raw(data.cast()) };
        check_registry_reentry(&probe);
        drop(probe);
    }

    unsafe fn wake_reentrant_waker_by_ref(data: *const ()) {
        let probe = unsafe { &*data.cast::<ReentrantProbe>() };
        check_registry_reentry(probe);
    }

    unsafe fn drop_reentrant_waker(data: *const ()) {
        let probe = unsafe { Arc::<ReentrantProbe>::from_raw(data.cast()) };
        check_registry_reentry(&probe);
        drop(probe);
    }

    static REENTRANT_VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_reentrant_waker,
        wake_reentrant_waker,
        wake_reentrant_waker_by_ref,
        drop_reentrant_waker,
    );

    #[test]
    fn dropping_last_resource_use_wakes_registered_scope_waiter() {
        let scope = OperationScope::new(
            OperationId::new(1),
            ScopeLimits::default(),
            ScopePolicy::FailFast,
            CleanupScheduler::new(1),
            OperationContext::new(ContextOptions::default()),
        );
        let id = scope.register_resource(ReadyCleanup).unwrap();
        let use_lease = scope.acquire_use(id).unwrap();
        scope.close_for_use(id).unwrap();
        let seen = scope.resource_epoch();
        let mut wait = Box::pin(scope.wait_for_resource_change(seen));
        let wake_count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_count));
        let mut cx = Context::from_waker(&waker);
        assert!(wait.as_mut().poll(&mut cx).is_pending());
        drop(use_lease);
        assert!(wake_count.0.load(Ordering::SeqCst) > 0);
        assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Ready(())));
    }

    #[test]
    fn opposing_scope_resource_transfers_do_not_invert_handle_locks() {
        let make_scope = |id| {
            OperationScope::new(
                OperationId::new(id),
                ScopeLimits::default(),
                ScopePolicy::FailFast,
                CleanupScheduler::new(1),
                OperationContext::new(ContextOptions::default()),
            )
        };
        let first = make_scope(10);
        let second = make_scope(11);
        let first_resource = first.register_resource(ReadyCleanup).unwrap();
        let second_resource = second.register_resource(ReadyCleanup).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let (tx, rx) = std::sync::mpsc::channel();

        let first_scope = first.clone();
        let first_destination = second.clone();
        let first_barrier = Arc::clone(&barrier);
        let first_tx = tx.clone();
        std::thread::spawn(move || {
            first_barrier.wait();
            first_tx
                .send(first_scope.transfer_resource_to(first_resource, &first_destination))
                .unwrap();
        });

        let second_scope = second.clone();
        let second_destination = first.clone();
        let second_barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            second_barrier.wait();
            tx.send(second_scope.transfer_resource_to(second_resource, &second_destination))
                .unwrap();
        });

        barrier.wait();
        let first_result = rx.recv_timeout(std::time::Duration::from_secs(1));
        let second_result = rx.recv_timeout(std::time::Duration::from_secs(1));
        assert!(first_result.is_ok(), "first opposing transfer must complete");
        assert!(second_result.is_ok(), "second opposing transfer must complete");
        assert!(first_result.unwrap().is_ok());
        assert!(second_result.unwrap().is_ok());
    }

    #[test]
    fn resource_waiter_waker_callbacks_can_reenter_registry_without_lock() {
        let scope = OperationScope::new(
            OperationId::new(12),
            ScopeLimits::default(),
            ScopePolicy::FailFast,
            CleanupScheduler::new(1),
            OperationContext::new(ContextOptions::default()),
        );
        let id = scope.register_resource(ReadyCleanup).unwrap();
        let use_lease = scope.acquire_use(id).unwrap();
        scope.close_for_use(id).unwrap();
        let seen = scope.resource_epoch();
        let mut wait = Box::pin(scope.wait_for_resource_change(seen));
        let lock_timeouts = Arc::new(AtomicUsize::new(0));
        let probe = Arc::new(ReentrantProbe {
            registry: scope.resources.clone(),
            lock_timeouts: Arc::clone(&lock_timeouts),
        });
        let raw = RawWaker::new(Arc::into_raw(probe).cast(), &REENTRANT_VTABLE);
        let waker = unsafe { Waker::from_raw(raw) };
        let mut cx = Context::from_waker(&waker);
        assert!(wait.as_mut().poll(&mut cx).is_pending());
        drop(use_lease);
        assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Ready(())));
        assert_eq!(lock_timeouts.load(Ordering::SeqCst), 0);
    }
}

/// Business and release facts collected from registered children.
#[derive(Default)]
pub(crate) struct JoinFacts {
    pub failed_required: Option<ChildBusinessSummary>,
    pub business: Vec<ChildBusinessSummary>,
    pub settlements: Vec<ChildSettlement>,
}

pub(crate) fn poll_child_business(scope: &OperationScope, cx: &mut Context<'_>, facts: &mut JoinFacts) -> Poll<()> {
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
        if facts.business.iter().any(|summary| summary.operation_id == id) {
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
    if pending { Poll::Pending } else { Poll::Ready(()) }
}

pub(crate) fn poll_child_releases(scope: &OperationScope, cx: &mut Context<'_>, facts: &mut JoinFacts) -> Poll<()> {
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
        if facts.settlements.iter().any(|settlement| settlement.operation_id == id) {
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
    if pending { Poll::Pending } else { Poll::Ready(()) }
}

pub(crate) fn poll_scope_business(scope: &OperationScope, cx: &mut Context<'_>, facts: &mut JoinFacts) -> Poll<()> {
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

pub(crate) fn poll_scope_releases(scope: &OperationScope, cx: &mut Context<'_>, facts: &mut JoinFacts) -> Poll<()> {
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

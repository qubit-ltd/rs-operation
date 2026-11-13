// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! A scope owns child registration authority and coordinates child settlement.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use super::ChildRequirement;
use super::ChildReservation;
use super::ScopeError;
use super::ScopeJoinReport;
use super::ScopeOptions;
use super::ScopePolicy;
use super::internal::EffectState;
use super::internal::State;
use super::internal::poll::poll_scope_business;
use super::internal::poll::poll_scope_releases;
use super::join_facts::JoinFacts;
use crate::context::ContextError;
use crate::context::DeadlineSignal;
use crate::context::OperationContext;
use crate::protocol::OwnedCleanup;
use crate::report::EffectRecord;
use crate::report::OperationId;
use crate::report::RecoveryKey;
use crate::report::RecoveryOwner;
use crate::report::ReleaseReceipt;
use crate::report::ResourceId;
use crate::report::ScopeLimits;
use crate::report::StopReason;
use crate::resource::CleanupScheduler;
use crate::resource::DependencyError;
use crate::resource::RecoveryRegistry;
use crate::resource::RegisterError;
use crate::resource::RegistryError;
use crate::resource::ResourceError;
use crate::resource::ResourceHandle;
use crate::resource::ResourceRegistry;
use crate::resource::ResourceUse;
use crate::resource::TransferError;

static NEXT_SCOPE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1_000_000);

/// A scope that owns child registration authority and waits for registered
/// work.
///
/// Operation scopes are supplied to operation bodies by the operation runtime.
/// They let a body register structured children and resources before joining
/// them during settlement.
///
/// # Examples
///
/// ```
/// use qubit_operation::scope::OperationScope;
///
/// fn inspect_scope(scope: &OperationScope) {
///     let _policy = scope.policy();
/// }
/// ```
#[derive(Clone)]
pub struct OperationScope {
    /// Stable identifier shared by the operation's scope tree.
    pub(super) operation_id: OperationId,
    /// Child acceptance, reservations, and cancellation state.
    pub(super) state: Arc<Mutex<State>>,
    /// Immutable resource and child-operation limits for this scope.
    pub(super) limits: ScopeLimits,
    /// Immutable child failure policy selected at construction.
    pub(super) policy: ScopePolicy,
    /// Registry containing resources owned by this scope.
    pub(super) resources: ResourceRegistry,
    /// Handles required to acquire, close, or transfer owned resources.
    pub(super) handles: Arc<Mutex<BTreeMap<ResourceId, ResourceHandle>>>,
    /// Scheduler shared by this scope and its descendants.
    pub(super) cleanup: CleanupScheduler,
    /// Store for owners returned by cleanup recovery.
    pub(super) recovery: RecoveryRegistry,
    /// One-use recovery keys made available to the supervising operation.
    pub(super) recovery_keys: Arc<Mutex<Vec<RecoveryKey<RecoveryOwner>>>>,
    /// Context inherited by child scopes.
    pub(super) context: OperationContext,
    /// Lazily polled deadline notification, shared by scope clones.
    pub(super) deadline_signal: Arc<Mutex<Option<DeadlineSignal>>>,
    /// Bounded side-effect facts shared by this scope tree.
    pub(super) effects: Arc<Mutex<EffectState>>,
}

impl OperationScope {
    /// Build a scope from the operation's limits, policy, cleanup scheduler,
    /// and context.
    ///
    /// # Parameters
    ///
    /// * `operation` - Stable identifier assigned to this operation.
    /// * `limits` - Resource, child-operation, and effect-record limits.
    /// * `policy` - Policy applied when a required child fails.
    /// * `cleanup` - Scheduler shared with the operation's child scopes.
    /// * `context` - Operation context inherited by child scopes.
    ///
    /// # Returns
    ///
    /// Returns a scope initialized with the supplied operation configuration.
    #[must_use]
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

    /// Return the identity of this operation scope.
    ///
    /// # Returns
    ///
    /// Returns the operation identifier shared by this scope tree.
    #[must_use]
    #[inline]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// The immutable policy selected at scope creation.
    ///
    /// # Returns
    ///
    /// Returns the policy applied to required child failures.
    #[must_use]
    #[inline]
    pub fn policy(&self) -> ScopePolicy {
        self.policy
    }

    /// Check whether this scope still owns a registration or source tombstone.
    ///
    /// # Parameters
    ///
    /// * `id` - Resource identifier to check.
    ///
    /// # Returns
    ///
    /// Returns whether this scope retains the registration or its source
    /// tombstone.
    #[must_use]
    pub fn contains_resource(&self, id: ResourceId) -> bool {
        self.resources.contains(id)
    }

    /// Create an owned child scope whose accepted work and resources join this
    /// scope.
    ///
    /// # Parameters
    ///
    /// * `options` - Child context, limits, and policy; limits cannot exceed
    ///   this scope's limits.
    ///
    /// # Returns
    ///
    /// Returns the registered child scope with the requested context and
    /// bounded limits.
    ///
    /// # Errors
    ///
    /// Returns `ScopeError` when the context is invalid, this scope is closed,
    /// or a limit is exceeded.
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

    /// Retain a caller-declared side-effect fact for the final settlement
    /// report.
    ///
    /// # Parameters
    ///
    /// * `effect` - Fact to retain in the bounded report.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when the fact is retained, or an error when the report
    /// limit is full.
    ///
    /// # Errors
    ///
    /// Returns `ScopeError::EffectLimitReached` when the configured record
    /// limit is full.
    pub fn record_effect(&self, effect: EffectRecord) -> Result<(), ScopeError> {
        let mut state = self.effects.lock().unwrap_or_else(|e| e.into_inner());
        if state.records.len() >= state.limit {
            state.overflow = state.overflow.saturating_add(1);
            return Err(ScopeError::EffectLimitReached);
        }
        state.records.push(effect);
        Ok(())
    }

    /// Register a cleanup owner before creating or exposing its resource.
    ///
    /// # Type Parameters
    ///
    /// * `C` - Owned cleanup implementation stored by the resource registry.
    ///
    /// # Parameters
    ///
    /// * `owner` - Cleanup owner whose resource is registered.
    ///
    /// # Returns
    ///
    /// Returns the identifier assigned to the newly registered resource.
    ///
    /// # Errors
    ///
    /// Returns the registry error when registration cannot be completed.
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
    ///
    /// # Parameters
    ///
    /// * `id` - Identifier returned by `register_resource`.
    ///
    /// # Returns
    ///
    /// Returns the exclusive use authorization for the registered resource.
    ///
    /// # Errors
    ///
    /// Returns an error if the resource is absent or no longer accepts use.
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
    ///
    /// # Parameters
    ///
    /// * `id` - Identifier returned by `register_resource`.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when future use is closed for the resource.
    ///
    /// # Errors
    ///
    /// Returns an error if the resource is not registered in this scope.
    pub fn close_for_use(&self, id: ResourceId) -> Result<(), ResourceError> {
        self.handles
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .ok_or(ResourceError::NotRegistered)?
            .close_for_use()
    }

    /// Order one resource's cleanup after a prerequisite resource is released.
    ///
    /// # Parameters
    ///
    /// * `resource` - Resource whose cleanup must wait.
    /// * `prerequisite` - Resource that must be released first.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when the cleanup dependency is recorded.
    ///
    /// # Errors
    ///
    /// Returns an error if either resource is absent or the dependency is
    /// invalid.
    pub fn cleanup_after(&self, resource: ResourceId, prerequisite: ResourceId) -> Result<(), DependencyError> {
        let handles = self.handles.lock().unwrap_or_else(|e| e.into_inner());
        handles
            .get(&resource)
            .ok_or(DependencyError::NotRegistered)?
            .cleanup_after(handles.get(&prerequisite).ok_or(DependencyError::NotRegistered)?)
    }

    /// Transfer a resource and its connected dependency group to another scope
    /// atomically.
    ///
    /// # Parameters
    ///
    /// * `id` - Identifier of a resource in this scope.
    /// * `destination` - Scope that will own the transferred group.
    ///
    /// # Returns
    ///
    /// Returns the resource identifier assigned in the destination scope.
    ///
    /// # Errors
    ///
    /// Returns an error if transfer validation fails or both scopes share a
    /// registry.
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

    /// Confirm release of a transferred owner using the receiver's release
    /// proof.
    ///
    /// # Parameters
    ///
    /// * `id` - Source registration identifier.
    /// * `receipt` - Proof emitted by the receiving registry after release.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` after the transferred release is confirmed.
    ///
    /// # Errors
    ///
    /// Returns an error when the receipt does not confirm this transferred
    /// resource.
    pub fn confirm_transferred_release(&self, id: ResourceId, receipt: ReleaseReceipt) -> Result<(), RegistryError> {
        self.resources.confirm_transferred_released(id, receipt)?;
        self.handles.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
        self.resources.notify_change();
        Ok(())
    }

    /// Reserve a child registration slot before handing work to a domain
    /// executor.
    ///
    /// # Parameters
    ///
    /// * `requirement` - Whether the eventual child contributes to
    ///   required-failure reporting.
    ///
    /// # Returns
    ///
    /// Returns a reservation that can commit one child registration within this
    /// scope.
    ///
    /// # Errors
    ///
    /// Returns an error when this scope is closed or its child limit is
    /// reached.
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
    ///
    /// # Parameters
    ///
    /// * `reason` - First stop reason recorded for this scope tree.
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

    /// Close this scope's body submissions and wait for every committed child
    /// to settle.
    ///
    /// # Returns
    ///
    /// Returns settlement details and the identifiers of failed required
    /// children.
    pub async fn join(&self) -> ScopeJoinReport {
        self.close_body();
        self.close_body_resources();
        let mut facts = JoinFacts::default();
        std::future::poll_fn(|cx| poll_scope_business(self, cx, &mut facts)).await;
        std::future::poll_fn(|cx| poll_scope_releases(self, cx, &mut facts)).await;
        let _settlement = crate::operation::internal::cleanup_scope_tree(self).await;
        ScopeJoinReport {
            settlements: facts.settlements,
            required_failures: facts.failed_required.into_iter().collect(),
        }
    }
    /// Take a one-use typed recovery key produced by cleanup settlement.
    ///
    /// # Returns
    ///
    /// Returns the next recovery key, or `None` when no key is available.
    pub fn take_recovery_key(&self) -> Option<RecoveryKey<RecoveryOwner>> {
        self.recovery_keys.lock().unwrap_or_else(|e| e.into_inner()).pop()
    }

    /// Recover an owner only with its original registry-issued key.
    ///
    /// # Type Parameters
    ///
    /// * `R` - Concrete owner type represented by the key.
    ///
    /// # Parameters
    ///
    /// * `key` - One-use key issued for the owner being recovered.
    ///
    /// # Returns
    ///
    /// Returns the owner on success, or the unchanged key when it cannot be
    /// recovered.
    pub fn take_recovery<R: Send + 'static>(&self, key: RecoveryKey<R>) -> Result<R, RecoveryKey<R>> {
        self.recovery.take_recovery(key)
    }

    /// Clone the retained effects and return the number omitted after the
    /// limit.
    ///
    /// # Returns
    ///
    /// Returns the retained effect records and the number omitted by the record
    /// limit.
    #[must_use]
    pub(crate) fn effect_snapshot(&self) -> (Arc<[EffectRecord]>, usize) {
        let state = self.effects.lock().unwrap_or_else(|e| e.into_inner());
        (Arc::from(state.records.clone()), state.overflow)
    }

    /// Clone the registry used to track this scope's resource ownership.
    ///
    /// # Returns
    ///
    /// Returns a clone of the resource registry handle.
    #[must_use]
    #[inline]
    pub(crate) fn resource_registry(&self) -> ResourceRegistry {
        self.resources.clone()
    }
    /// Clone the cleanup scheduler shared with this scope's descendants.
    ///
    /// # Returns
    ///
    /// Returns a clone of the shared cleanup scheduler handle.
    #[must_use]
    #[inline]
    pub(crate) fn cleanup_scheduler(&self) -> CleanupScheduler {
        self.cleanup.clone()
    }
    /// Clone the registry that stores recoverable cleanup owners.
    ///
    /// # Returns
    ///
    /// Returns a clone of the recovery registry handle.
    #[must_use]
    #[inline]
    pub(crate) fn recovery_registry(&self) -> RecoveryRegistry {
        self.recovery.clone()
    }
    /// Read the resource registry change counter used by cleanup waiters.
    ///
    /// # Returns
    ///
    /// Returns the registry's current change counter.
    #[must_use]
    #[inline]
    pub(crate) fn resource_epoch(&self) -> u64 {
        self.resources.change_epoch()
    }

    /// Snapshot the currently registered child scopes for recursive lifecycle
    /// work.
    ///
    /// # Returns
    ///
    /// Returns cloned handles for the currently registered child scopes.
    #[must_use]
    pub(crate) fn child_scopes(&self) -> Vec<OperationScope> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .child_scopes
            .clone()
    }

    /// Close new uses for every resource when the operation body finishes.
    pub(crate) fn close_body_resources(&self) {
        let handles = self.handles.lock().unwrap_or_else(|e| e.into_inner());
        for handle in handles.values() {
            let _ = handle.close_for_use();
        }
        let _ = (&self.cleanup, &self.resources, &self.recovery);
    }

    /// Poll this scope's deadline and recursively cancel expired child scopes.
    ///
    /// # Parameters
    ///
    /// * `cx` - Task context used to register the deadline notification waker.
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

    /// Retain a recovery key so the supervising operation can return the owner.
    ///
    /// # Parameters
    ///
    /// * `key` - Recovery key issued for the owner being returned.
    pub(crate) fn save_recovery_key(&self, key: RecoveryKey<RecoveryOwner>) {
        self.recovery_keys.lock().unwrap_or_else(|e| e.into_inner()).push(key);
    }
    /// Wait until resources change or a cleanup becomes ready or completes.
    ///
    /// # Parameters
    ///
    /// * `seen` - Previously observed resource change counter.
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

    /// Move an already validated resource while both handle maps are locked in
    /// stable order.
    ///
    /// # Parameters
    ///
    /// * `id` - Source registration identifier already validated by the caller.
    /// * `destination` - Scope receiving the resource.
    /// * `source` - Locked source handle map.
    /// * `target` - Locked destination handle map.
    ///
    /// # Returns
    ///
    /// Returns the identifier assigned to the transferred resource.
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
}

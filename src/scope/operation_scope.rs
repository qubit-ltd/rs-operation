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

use crate::context::ContextError;
use crate::context::ContextOptions;
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
use crate::resource::RecoveryRegistry;
use crate::resource::RegisterError;
use crate::resource::RegistryError;
use crate::resource::ResourceError;
use crate::resource::ResourceHandle;
use crate::resource::ResourceRegistry;
use crate::resource::ResourceUse;
use crate::resource::TransferError;

use super::internal::EffectState;
use super::internal::State;
use super::internal::poll::poll_scope_business;
use super::internal::poll::poll_scope_releases;
use super::join_facts::JoinFacts;
use super::{ChildRequirement, ScopeError, ScopeJoinReport, ScopeOptions, ScopePolicy};

static NEXT_SCOPE_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1_000_000);

/// A scope that owns child registration authority and waits for registered
/// work.
#[derive(Clone)]
pub struct OperationScope {
    pub(super) operation_id: OperationId,
    pub(super) state: Arc<Mutex<State>>,
    pub(super) limits: ScopeLimits,
    pub(super) policy: ScopePolicy,
    pub(super) resources: ResourceRegistry,
    pub(super) handles: Arc<Mutex<BTreeMap<ResourceId, ResourceHandle>>>,
    pub(super) cleanup: CleanupScheduler,
    pub(super) recovery: RecoveryRegistry,
    pub(super) recovery_keys: Arc<Mutex<Vec<RecoveryKey<RecoveryOwner>>>>,
    pub(super) context: OperationContext,
    pub(super) deadline_signal: Arc<Mutex<Option<DeadlineSignal>>>,
    pub(super) effects: Arc<Mutex<EffectState>>,
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
        let child_id = OperationId::new(
            NEXT_SCOPE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u128,
        );
        let mut child = OperationScope::new(
            child_id,
            options.limits,
            options.policy,
            self.cleanup.clone(),
            context,
        );
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
            let mut target = destination
                .handles
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            self.transfer_with_locked_handles(id, destination, &mut source, &mut target)
        } else {
            let mut target = destination
                .handles
                .lock()
                .unwrap_or_else(|e| e.into_inner());
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

    /// Confirm release of a transferred owner using the receiver's release
    /// proof.
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
        let mut signal = self
            .deadline_signal
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(mut signal) = signal.take() {
            if Pin::new(&mut signal).poll(cx).is_ready() {
                self.request_cancel(StopReason::Timeout);
            } else {
                *self
                    .deadline_signal
                    .lock()
                    .unwrap_or_else(|e| e.into_inner()) = Some(signal);
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
        self.recovery_keys
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(key);
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
    pub fn reserve_child(
        &self,
        requirement: ChildRequirement,
    ) -> Result<ChildReservation, ScopeError> {
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

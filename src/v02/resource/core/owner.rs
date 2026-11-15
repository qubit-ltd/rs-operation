// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use super::internal::DefaultDropJob;
use super::internal::Slot;
use super::internal::finish_async_cleanup;
use super::internal::lock;
use super::scope_completion::ScopeCompletion;
use crate::v02::context::CleanupContext;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceLookupError;
use crate::v02::resource::cleanup::AsyncCleanup;
use crate::v02::resource::cleanup::AsyncCleanupJob;
use crate::v02::resource::cleanup_port::CleanupDispatchPermit;

/// Values atomically taken from an owner once scope closure and zero users
/// coincide.
///
/// # Type Parameters
///
/// - `T`: Resource value transferred to the cleanup job.
type ReadyCleanup<T> = (
    T,
    ResourceRegistration,
    Box<dyn CleanupDispatchPermit>,
    ResourceId,
    Arc<ScopeCompletion>,
    Option<Arc<dyn AsyncCleanup<T>>>,
);

/// Owns a resource until its scope has ended and every user releases its
/// reference.
///
/// The mutex-protected slot rejects new acquisitions after closure, keeps the
/// resource alive for existing users, and transfers it to the reserved cleanup
/// queue only after both lifecycle conditions are satisfied.
///
/// # Type Parameters
///
/// - `T`: Resource value type retained until cleanup is dispatched.
pub(in crate::v02::resource) struct Owner<T> {
    /// Resource, registration, cleanup permit, and active-user state.
    state: Mutex<Slot<T>>,
}
impl<T: Send + Sync + 'static> Owner<T> {
    /// Creates a shared owner with resource capacity and cleanup already
    /// reserved.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Thread-safe resource type owned by this slot.
    ///
    /// # Parameters
    ///
    /// - `value`: Resource value retained until cleanup dispatch.
    /// - `generation`: Key generation accepted by this owner.
    /// - `registration`: Capacity registration held through cleanup or
    ///   recovery.
    /// - `cleanup_permit`: Reserved queue slot used to enqueue cleanup without
    ///   losing the resource to backpressure.
    /// - `resource_id`: Stable identity attached to cleanup reports.
    /// - `completion`: Scope settlement state notified when cleanup completes.
    /// - `policy`: Optional asynchronous cleanup policy for the resource.
    ///
    /// # Returns
    ///
    /// A shared owner with zero users and an open acquisition state.
    #[must_use = "the resource owner must be retained for its lifetime"]
    pub(in crate::v02::resource::core) fn new(
        value: T,
        generation: u64,
        registration: ResourceRegistration,
        cleanup_permit: Box<dyn CleanupDispatchPermit>,
        resource_id: ResourceId,
        completion: Arc<ScopeCompletion>,
        policy: Option<Arc<dyn AsyncCleanup<T>>>,
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

    /// Clones the resource's shared value while it is still owned by this slot.
    ///
    /// The returned `Arc` keeps the value alive but does not increment the
    /// owner's counted user set. A caller must pair it with a counted owner
    /// reference before exposing the clone to a resource user.
    ///
    /// # Parameters
    ///
    /// - `self`: Owner whose value is borrowed and cloned.
    ///
    /// # Returns
    ///
    /// `Some` with a shared value while it remains in the slot, or `None` after
    /// cleanup has taken ownership.
    pub(in crate::v02::resource) fn value_arc(&self) -> Option<Arc<T>> {
        lock(&self.state).value.as_ref().map(Arc::clone)
    }

    /// Reports whether this owner still has active resource users.
    ///
    /// # Parameters
    ///
    /// - `self`: Owner whose user count is checked.
    ///
    /// # Returns
    ///
    /// `true` while at least one user reference remains.
    #[must_use = "the active-user state should be inspected"]
    pub(in crate::v02::resource::core) fn has_users(&self) -> bool {
        lock(&self.state).users != 0
    }

    /// Acquires a user reference while the owner remains open.
    ///
    /// The reference increments the user count, which prevents cleanup from
    /// taking the resource until that reference is released.
    ///
    /// # Parameters
    ///
    /// - `self`: Shared owner whose resource is acquired.
    /// - `generation`: Key generation that must match the owner.
    ///
    /// # Returns
    ///
    /// A shared owner reference representing one active resource user.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceLookupError::StaleKey`] for a mismatched generation or
    /// [`ResourceLookupError::ScopeEnded`] after the owner is closed.
    pub(in crate::v02::resource) fn acquire(
        self: &Arc<Self>,
        generation: u64,
    ) -> Result<Arc<Self>, ResourceLookupError> {
        let mut state = lock(&self.state);
        if state.key_generation != generation {
            return Err(ResourceLookupError::StaleKey);
        }
        if !state.open || state.value.is_none() {
            return Err(ResourceLookupError::ScopeEnded);
        }
        state.users += 1;
        Ok(Arc::clone(self))
    }

    /// Creates another user reference from a reference that is already active.
    ///
    /// Unlike [`Self::acquire`], cloning remains valid after the scope closes,
    /// provided at least one existing user still keeps the resource alive.
    ///
    /// # Parameters
    ///
    /// - `self`: Shared owner backing the existing resource reference.
    /// - `generation`: Key generation that must match this owner.
    ///
    /// # Returns
    ///
    /// A new shared owner reference counted as another active user.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceLookupError::StaleKey`] for a mismatched generation or
    /// [`ResourceLookupError::ScopeEnded`] when no active users or value
    /// remains.
    pub(in crate::v02::resource) fn clone_user(
        self: &Arc<Self>,
        generation: u64,
    ) -> Result<Arc<Self>, ResourceLookupError> {
        let mut state = lock(&self.state);
        if state.key_generation != generation {
            return Err(ResourceLookupError::StaleKey);
        }
        if state.users == 0 || state.value.is_none() {
            return Err(ResourceLookupError::ScopeEnded);
        }
        state.users += 1;
        Ok(Arc::clone(self))
    }

    /// Releases one active user and dispatches cleanup if this was the last
    /// one.
    ///
    /// The resource is moved out only after scope closure and a zero user
    /// count; until then the owner remains available to existing users.
    ///
    /// # Parameters
    ///
    /// - `self`: Owner whose user count is decremented.
    ///
    /// # Panics
    ///
    /// Panics in builds with overflow checks if called without a matching
    /// active acquisition, violating the internal user-count invariant.
    pub(in crate::v02::resource) fn release(&self) {
        let ready = {
            let mut state = lock(&self.state);
            state.users -= 1;
            Self::take_if_ready(&mut state)
        };
        Self::dispatch(ready);
    }

    /// Closes the owner to new users and dispatches cleanup when no users
    /// remain.
    ///
    /// Existing references stay valid after closure and defer cleanup until
    /// each one is released.
    ///
    /// # Parameters
    ///
    /// - `self`: Owner whose scope is ending.
    pub(in crate::v02::resource) fn end(&self) {
        let ready = {
            let mut state = lock(&self.state);
            state.open = false;
            Self::take_if_ready(&mut state)
        };
        Self::dispatch(ready);
    }

    /// Takes ownership of cleanup data once the owner is closed and unused.
    ///
    /// Leaves the slot unchanged while admission remains open or a user
    /// remains. On readiness, removes the value, registration, reserved
    /// cleanup permit, identity, completion state, and optional cleanup
    /// policy as one transition.
    ///
    /// # Parameters
    ///
    /// - `state`: Mutex-protected slot state to inspect and update.
    ///
    /// # Returns
    ///
    /// `Some` with all cleanup inputs when the owner is closed and has no
    /// users; otherwise `None`.
    ///
    /// # Panics
    ///
    /// Panics if the value, registration, or cleanup permit is missing, or if
    /// the resource user count does not match the value's strong references.
    fn take_if_ready(state: &mut Slot<T>) -> Option<ReadyCleanup<T>> {
        if state.open || state.users != 0 {
            return None;
        }
        let value = Arc::into_inner(state.value.take().expect("ready owner retains value"))
            .expect("resource user count matches strong references");
        Some((
            value,
            state.registration.take().expect("owner holds resource registration"),
            state
                .cleanup_permit
                .take()
                .expect("owner holds cleanup dispatch permit"),
            state.resource_id,
            Arc::clone(&state.completion),
            state.policy.take(),
        ))
    }

    /// Starts scope settlement and submits an eligible resource to cleanup.
    ///
    /// Dispatches asynchronous-policy jobs through `AsyncCleanupJob` and
    /// default destructors through `DefaultDropJob`, using the permit reserved
    /// before resource admission.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Resource type being transferred to cleanup.
    ///
    /// # Parameters
    ///
    /// - `ready`: Cleanup inputs returned by [`Self::take_if_ready`], or `None`
    ///   when closure and final-user release have not both occurred.
    ///
    /// # Panics
    ///
    /// Panics if the empty cleanup context cannot be constructed, which would
    /// violate its constructor's invariant.
    fn dispatch(ready: Option<ReadyCleanup<T>>) {
        if let Some((value, registration, permit, resource_id, completion, policy)) = ready {
            completion.start();
            if let Some(policy) = policy {
                let context = CleanupContext::new(Default::default()).expect("empty cleanup context is valid");
                let result = Arc::new(Mutex::new(None));
                let completion_for_job = Arc::clone(&completion);
                permit.enqueue(Box::new(
                    AsyncCleanupJob::new(value, policy, context, result)
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

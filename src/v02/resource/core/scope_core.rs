// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use super::super::ResourceRef;
use super::super::ResourceReservation;
use super::Owner;
use super::ScopeCompletion;
use super::internal::Entry;
use super::internal::State;
use super::internal::lock;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::report::ManagerId;
use crate::v02::report::ResourceCapacityError;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceManageErrorKind;
use crate::v02::report::ResourceScopeId;
use crate::v02::resource::ResourceCommitError;
use crate::v02::resource::ResourceCommitWithError;
use crate::v02::resource::cleanup::AsyncCleanup;
use crate::v02::resource::cleanup_port::CleanupDispatch;
use crate::v02::resource::cleanup_port::CleanupDispatchPermit;

/// Coordinates resource admission, ownership, and cleanup for one resource
/// scope.
///
/// The core holds the scope's capacity pool and cleanup dispatcher, while its
/// mutex-protected state tracks registrations and typed resource owners. Every
/// accepted resource reserves both resource capacity and a cleanup queue slot
/// before ownership is committed.
pub(in crate::v02::resource) struct ScopeCore {
    /// Manager that owns this scope and its resource registrations.
    pub(in crate::v02::resource) manager: ManagerId,
    /// Stable identity used to reject reservations from another resource scope.
    pub(in crate::v02::resource) id: ResourceScopeId,
    /// Capacity hierarchy charged for each resource registered in the scope.
    pub(in crate::v02::resource::core) capacity: Arc<ScopeCapacity>,
    /// Dispatcher that reserves a bounded cleanup slot before resource
    /// admission.
    pub(in crate::v02::resource::core) cleanup: Arc<dyn CleanupDispatch>,
    /// Shared settlement tracker notified as resources are released or cleaned.
    pub(in crate::v02::resource::core) completion: Arc<ScopeCompletion>,
    /// Admission state, resource owners, and reservation counts protected by a
    /// mutex.
    pub(in crate::v02::resource) state: Mutex<State>,
}
impl ScopeCore {
    /// Reserves resource capacity and a cleanup queue slot for a future commit.
    ///
    /// The returned reservation must be committed or dropped. If the scope ends
    /// while capacity is being acquired, both permits are released before the
    /// error is returned.
    ///
    /// # Parameters
    ///
    /// - `self`: Scope core accepting the resource registration.
    ///
    /// # Returns
    ///
    /// A reservation holding both resource capacity and cleanup dispatch
    /// capacity.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceCapacityError::ScopeEnded`] if admission is closed, or
    /// [`ResourceCapacityError::CapacityExceeded`] if either required capacity
    /// permit cannot be acquired.
    pub(in crate::v02::resource) fn reserve(self: &Arc<Self>) -> Result<ResourceReservation, ResourceCapacityError> {
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
        Ok(ResourceReservation::new(Arc::clone(self), registration, cleanup_permit))
    }

    /// Releases a reservation that will not be converted into a retained owner.
    ///
    /// When the scope is already closed, updates settlement accounting so this
    /// abandoned reservation does not keep the final report pending.
    ///
    /// # Parameters
    ///
    /// - `self`: Scope core whose in-progress reservation count is decremented.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if no reservation was recorded, indicating an
    /// internal accounting error.
    pub(in crate::v02::resource) fn release_reservation(self: &Arc<Self>) {
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

    /// Commits a reserved resource and returns a reference owned by this scope.
    ///
    /// The reservation is consumed on success and its capacity and cleanup
    /// permits move into the resource owner. On failure, the error retains the
    /// original value and reservation so the caller can recover or retry.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Thread-safe resource type owned by the scope.
    ///
    /// # Parameters
    ///
    /// - `self`: Scope core that owns the committed resource.
    /// - `value`: Resource value to retain in the scope.
    /// - `reservation`: Matching admission reservation to consume.
    ///
    /// # Returns
    ///
    /// A `ResourceRef<T>` for accessing the newly retained value.
    ///
    /// # Errors
    ///
    /// Returns the value and reservation if the scope has ended, the
    /// reservation belongs to another scope, or slot allocation is
    /// exhausted.
    ///
    /// # Panics
    ///
    /// Panics if a newly created owner violates its internal value or
    /// open-state invariants.
    pub(in crate::v02::resource) fn commit_reserved<T: Send + Sync + 'static>(
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
        state.slots.insert(slot, Entry { owner: Box::new(owner) });
        Ok(ResourceRef {
            owner: owner_ref,
            value: Some(value),
            generation,
            manager: self.manager,
            scope: self.id,
            slot,
        })
    }

    /// Commits a resource with a policy for asynchronous cleanup.
    ///
    /// The cleanup policy is retained with the owner and invoked when scope
    /// closure and the final resource reference make cleanup eligible. The
    /// reservation is consumed on success; failures preserve the value, policy,
    /// and reservation in the returned error.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Thread-safe resource type owned by the scope.
    /// - `C`: Asynchronous cleanup policy for `T`.
    ///
    /// # Parameters
    ///
    /// - `self`: Scope core that owns the committed resource.
    /// - `value`: Resource value to retain in the scope.
    /// - `cleanup`: Policy run when the resource becomes eligible for cleanup.
    /// - `reservation`: Matching admission reservation to consume.
    ///
    /// # Returns
    ///
    /// A `ResourceRef<T>` for accessing the newly retained value.
    ///
    /// # Errors
    ///
    /// Returns the value, cleanup policy, and reservation if the scope has
    /// ended, the reservation belongs to another scope, or slot allocation
    /// is exhausted.
    ///
    /// # Panics
    ///
    /// Panics if a newly created owner violates its internal value or
    /// open-state invariants.
    pub(in crate::v02::resource) fn commit_reserved_with<T, C>(
        self: &Arc<Self>,
        value: T,
        cleanup: C,
        mut reservation: ResourceReservation,
    ) -> Result<ResourceRef<T>, ResourceCommitWithError<T, C>>
    where
        T: Send + Sync + 'static,
        C: AsyncCleanup<T>,
    {
        let mut state = lock(&self.state);
        let (slot, generation, registration, cleanup_permit) =
            match self.take_commit_parts(&mut state, &mut reservation) {
                Ok(parts) => parts,
                Err(reason) => {
                    return Err(ResourceCommitWithError::new(reason, value, cleanup, reservation));
                }
            };
        let resource_id = ResourceId::new(self.manager, slot, generation);
        let policy: Arc<dyn AsyncCleanup<T>> = Arc::new(cleanup);
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
        state.slots.insert(slot, Entry { owner: Box::new(owner) });
        Ok(ResourceRef {
            owner: owner_ref,
            value: Some(value),
            generation,
            manager: self.manager,
            scope: self.id,
            slot,
        })
    }

    /// Validates a reservation and transfers its permits into a new resource
    /// owner.
    ///
    /// Advances the scope's slot counter and decrements its in-progress
    /// reservation count. The caller must hold the state mutex for the duration
    /// of this operation so closure cannot race the commit.
    ///
    /// # Parameters
    ///
    /// - `self`: Scope that must own the reservation.
    /// - `state`: Locked scope state updated by committing the reservation.
    /// - `reservation`: Reservation whose capacity and cleanup permits are
    ///   moved.
    ///
    /// # Returns
    ///
    /// The slot, current generation, resource registration, and cleanup permit
    /// for constructing the owner.
    ///
    /// # Errors
    ///
    /// Returns [`ResourceManageErrorKind::ScopeEnded`] for a foreign, closed,
    /// or already consumed reservation, or
    /// [`ResourceManageErrorKind::CapacityExceeded`] if no further slot
    /// identifier can be allocated.
    ///
    /// # Panics
    ///
    /// Panics if an active reservation has lost its resource registration or
    /// cleanup permit, violating the reservation invariant.
    fn take_commit_parts(
        self: &Arc<Self>,
        state: &mut State,
        reservation: &mut ResourceReservation,
    ) -> Result<(u64, u64, ResourceRegistration, Box<dyn CleanupDispatchPermit>), ResourceManageErrorKind> {
        if !Arc::ptr_eq(self, &reservation.scope) {
            return Err(ResourceManageErrorKind::ScopeEnded);
        }
        if !state.open || !reservation.tracked {
            return Err(ResourceManageErrorKind::ScopeEnded);
        }
        let slot = state.next_slot;
        let next_slot = slot.checked_add(1).ok_or(ResourceManageErrorKind::CapacityExceeded)?;
        let registration = reservation
            .registration
            .take()
            .expect("reservation has resource permit");
        let cleanup_permit = reservation.cleanup_permit.take().expect("reservation has cleanup slot");
        state.next_slot = next_slot;
        state.reservations = state.reservations.saturating_sub(1);
        reservation.tracked = false;
        Ok((slot, state.generation, registration, cleanup_permit))
    }

    /// Retains a resource for cleanup when the scope ends, without returning a
    /// reference.
    ///
    /// The owner is closed immediately, so cleanup begins once its retained
    /// ownership permits dispatch. This is used for resources that belong to
    /// the scope but are not exposed to callers.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Thread-safe resource type retained by the scope.
    ///
    /// # Parameters
    ///
    /// - `self`: Scope core that retains the resource.
    /// - `value`: Resource value whose cleanup is attached to this scope.
    /// - `reservation`: Matching capacity and cleanup reservation to consume.
    ///
    /// # Returns
    ///
    /// The identity assigned to the retained resource.
    ///
    /// # Panics
    ///
    /// Panics if the reservation belongs to another scope, was already
    /// consumed, or the scope's slot identifier space is exhausted.
    pub(in crate::v02::resource) fn retain_reserved<T: Send + Sync + 'static>(
        self: &Arc<Self>,
        value: T,
        reservation: ResourceReservation,
    ) -> ResourceId {
        self.retain_reserved_inner(value, None, reservation)
    }

    /// Retains a resource with an asynchronous cleanup policy and no user
    /// reference.
    ///
    /// The owner closes immediately and invokes `cleanup` once its lifetime
    /// permits cleanup dispatch.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Thread-safe resource type retained by the scope.
    /// - `C`: Asynchronous cleanup policy for `T`.
    ///
    /// # Parameters
    ///
    /// - `self`: Scope core that retains the resource.
    /// - `value`: Resource value whose cleanup is attached to this scope.
    /// - `cleanup`: Policy invoked when the owner becomes eligible for cleanup.
    /// - `reservation`: Matching capacity and cleanup reservation to consume.
    ///
    /// # Returns
    ///
    /// The identity assigned to the retained resource.
    ///
    /// # Panics
    ///
    /// Panics if the reservation belongs to another scope, was already
    /// consumed, or the scope's slot identifier space is exhausted.
    pub(in crate::v02::resource) fn retain_reserved_with<T, C>(
        self: &Arc<Self>,
        value: T,
        cleanup: C,
        reservation: ResourceReservation,
    ) -> ResourceId
    where
        T: Send + Sync + 'static,
        C: AsyncCleanup<T>,
    {
        self.retain_reserved_inner(value, Some(Arc::new(cleanup)), reservation)
    }

    /// Installs a resource owner using an already acquired reservation.
    ///
    /// Updates reservation accounting under the scope-state mutex, then closes
    /// the owner because this retained-resource path does not return a user
    /// reference. The resource is therefore handed to cleanup when its owner
    /// state permits dispatch.
    ///
    /// # Type Parameters
    ///
    /// - `T`: Thread-safe resource type retained by the scope.
    ///
    /// # Parameters
    ///
    /// - `self`: Scope core that owns the resource.
    /// - `value`: Resource value to install.
    /// - `policy`: Optional asynchronous cleanup policy kept with the owner.
    /// - `reservation`: Matching capacity and cleanup permits to consume.
    ///
    /// # Returns
    ///
    /// The identity assigned to the new resource owner.
    ///
    /// # Panics
    ///
    /// Panics if the reservation belongs to another scope or was already
    /// consumed, or if slot identifier space is exhausted.
    fn retain_reserved_inner<T: Send + Sync + 'static>(
        self: &Arc<Self>,
        value: T,
        policy: Option<Arc<dyn AsyncCleanup<T>>>,
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
                reservation.cleanup_permit.take().expect("reservation has cleanup slot"),
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

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Bounded registry for atomic resource dependency-group transfers.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;

use super::internal::NEXT_TRANSFER_NONCE;
use super::internal::TRANSFER_TRANSACTION;
use super::internal::lock;
use crate::v02::report::ManagerId;
use crate::v02::report::ResourceId;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ResourceTransfer;
use crate::v02::report::ResourceTransferError;
use crate::v02::report::TransferReceipt;
use crate::v02::resource::dependency::DependencyGraph;

/// A bounded owner registry used by scope and recovery-group transfer paths.
/// The value stays in the source until destination checks have all passed.
///
/// # Type Parameters
///
/// * `T` — owner value stored in the registry and moved during transfer.
#[must_use = "keep the transfer registry alive; dropping it leaks remaining owners"]
pub(crate) struct TransferRegistry<T> {
    /// Manager that owns every resource in this registry.
    manager: ManagerId,
    /// Scope that currently owns the resources.
    scope: ResourceScopeId,
    /// Maximum number of resource owners accepted by this registry.
    capacity: usize,
    /// Dependency graph used to validate complete-group transfers.
    dependencies: Arc<DependencyGraph>,
    /// Resource owners indexed by their stable IDs.
    owners: Mutex<HashMap<ResourceId, T>>,
}

impl<T> Drop for TransferRegistry<T> {
    /// Leaks any remaining owners instead of dropping them outside manager
    /// cleanup.
    ///
    /// # Parameters
    ///
    /// - `self`: Registry whose remaining owners must stay alive if cleanup did
    ///   not remove them first.
    fn drop(&mut self) {
        let owners = self.owners.get_mut().unwrap_or_else(PoisonError::into_inner);
        for (_, owner) in owners.drain() {
            // The owning manager must end these resources through cleanup.
            // If its registry is dropped too early, do not run T::drop here.
            std::mem::forget(owner);
        }
    }
}

impl<T> TransferRegistry<T> {
    /// Creates an empty registry with the given scope capacity and dependency
    /// graph.
    ///
    /// # Parameters
    ///
    /// * `manager` — manager that owns resources in this registry.
    /// * `scope` — resource scope that owns the registry entries.
    /// * `capacity` — maximum number of owners accepted by the registry.
    /// * `dependencies` — graph used to validate dependency-group transfers.
    ///
    /// # Returns
    ///
    /// An empty registry for the specified manager and scope.
    ///
    /// # Panics
    ///
    /// Panics if `capacity` exceeds the map's allocation limit.
    pub(crate) fn new(
        manager: ManagerId,
        scope: ResourceScopeId,
        capacity: usize,
        dependencies: Arc<DependencyGraph>,
    ) -> Self {
        Self {
            manager,
            scope,
            capacity,
            dependencies,
            owners: Mutex::new(HashMap::with_capacity(capacity)),
        }
    }

    /// Returns the number of owners currently held by this registry.
    ///
    /// # Parameters
    ///
    /// * `self` — registry whose current owner count is read.
    ///
    /// # Returns
    ///
    /// The number of owners stored at the time of the call.
    #[must_use = "use the registry length to observe its owner count"]
    #[inline]
    pub(crate) fn len(&self) -> usize {
        lock(&self.owners).len()
    }

    /// Inserts an owner when its manager matches and capacity remains
    /// available.
    ///
    /// Returns the owner unchanged if the manager is wrong, the ID is already
    /// present, or the registry is full.
    ///
    /// # Parameters
    ///
    /// * `self` — registry that may accept the owner.
    /// * `id` — stable identity assigned to the owner.
    /// * `owner` — value to store while the registry admits it.
    ///
    /// # Returns
    ///
    /// `Ok(())` when the owner is stored; otherwise, the unchanged owner is
    /// returned in `Err`.
    pub(crate) fn insert(&self, id: ResourceId, owner: T) -> Result<(), T> {
        if id.manager() != self.manager {
            return Err(owner);
        }
        let mut owners = lock(&self.owners);
        if owners.len() >= self.capacity || owners.contains_key(&id) {
            return Err(owner);
        }
        owners.insert(id, owner);
        Ok(())
    }

    /// Transfers all listed members under one transaction. Capacity and
    /// membership failures leave the source map unchanged.
    ///
    /// # Parameters
    ///
    /// * `self` — source registry that currently owns the group.
    /// * `destination` — registry that will own the transferred members.
    /// * `members` — complete dependency group to move atomically.
    ///
    /// # Returns
    ///
    /// A transfer record and receipt after every member has moved.
    ///
    /// # Errors
    ///
    /// Returns an error if the registries have different managers or the same
    /// scope, membership is incomplete or duplicated, cleanup has started, or
    /// the destination lacks capacity. Failures leave both registries
    /// unchanged.
    ///
    /// # Panics
    ///
    /// Panics if a source entry disappears after membership validation, which
    /// would indicate an internal transfer invariant violation.
    pub(crate) fn transfer_group_to(
        &self,
        destination: &Self,
        members: &[ResourceId],
    ) -> Result<ResourceTransfer, ResourceTransferError> {
        if self.manager != destination.manager {
            return Err(ResourceTransferError::WrongManager);
        }
        if self.scope == destination.scope {
            return Err(ResourceTransferError::SameScope);
        }
        if members.is_empty() {
            return Err(ResourceTransferError::IncompleteDependencyGroup);
        }
        let _transaction = lock(&TRANSFER_TRANSACTION);
        let mut source = lock(&self.owners);
        let mut target = lock(&destination.owners);

        if members.iter().any(|id| id.manager() != self.manager) {
            return Err(ResourceTransferError::WrongManager);
        }
        if members.iter().any(|id| !source.contains_key(id)) {
            return Err(ResourceTransferError::IncompleteDependencyGroup);
        }
        if members.iter().any(|id| target.contains_key(id)) {
            return Err(ResourceTransferError::IncompleteDependencyGroup);
        }
        let unique = members.iter().copied().collect::<HashSet<_>>();
        if unique.len() != members.len() {
            return Err(ResourceTransferError::IncompleteDependencyGroup);
        }
        if self.dependencies.group_members(members[0]) != unique {
            return Err(ResourceTransferError::IncompleteDependencyGroup);
        }
        if !self.dependencies.can_transfer_group(&unique) {
            return Err(ResourceTransferError::CleanupStarted);
        }
        if target.len().saturating_add(members.len()) > destination.capacity {
            return Err(ResourceTransferError::CapacityExceeded);
        }

        target.reserve(members.len());
        let mut moved = Vec::with_capacity(members.len());
        for id in members {
            moved.push((*id, source.remove(id).expect("membership validated")));
        }
        for (id, owner) in moved {
            target.insert(id, owner);
        }

        let resource = members[0];
        let nonce = NEXT_TRANSFER_NONCE.fetch_add(1, Ordering::Relaxed);
        Ok(ResourceTransfer {
            members: members.to_vec().into(),
            source_scope: self.scope,
            target_scope: destination.scope,
            receipt: TransferReceipt::new(resource, self.scope, destination.scope, nonce),
        })
    }
}

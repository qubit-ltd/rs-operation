//! Atomic movement of complete resource dependency groups.
#![allow(dead_code)] // Public scope and RecoveryStore wiring is completed by T11.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::v02::report::{
    ManagerId, ResourceId, ResourceScopeId, ResourceTransfer, ResourceTransferError,
    TransferReceipt,
};
use crate::v02::resource::dependency::DependencyGraph;

static TRANSFER_TRANSACTION: Mutex<()> = Mutex::new(());
static NEXT_TRANSFER_NONCE: AtomicU64 = AtomicU64::new(1);

/// A bounded owner registry used by scope and recovery-group transfer paths.
/// The value stays in the source until destination checks have all passed.
pub(crate) struct TransferRegistry<T> {
    manager: ManagerId,
    scope: ResourceScopeId,
    capacity: usize,
    dependencies: Arc<DependencyGraph>,
    owners: Mutex<HashMap<ResourceId, T>>,
}

impl<T> Drop for TransferRegistry<T> {
    fn drop(&mut self) {
        let owners = self
            .owners
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, owner) in owners.drain() {
            // The owning manager must end these resources through cleanup.
            // If its registry is dropped too early, do not run T::drop here.
            std::mem::forget(owner);
        }
    }
}

impl<T> TransferRegistry<T> {
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

    pub(crate) fn len(&self) -> usize {
        lock(&self.owners).len()
    }

    /// Transfers all listed members under one transaction. Capacity and
    /// membership failures leave the source map unchanged.
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
        let unique = members
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
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

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::TransferRegistry;
    use crate::v02::report::{ManagerId, ResourceId, ResourceScopeId, ResourceTransferError};
    use crate::v02::resource::dependency::DependencyGraph;
    use std::sync::Arc;

    fn id(manager: ManagerId, slot: u64) -> ResourceId {
        ResourceId::new(manager, slot, 1)
    }

    #[test]
    fn dependency_group_transfer_is_atomic_and_returns_receipt() {
        let manager = ManagerId::new_unique();
        let source_scope = ResourceScopeId::new_unique();
        let destination_scope = ResourceScopeId::new_unique();
        let dependencies = Arc::new(DependencyGraph::default());
        assert!(
            dependencies
                .add(
                    id(manager, 2),
                    source_scope,
                    id(manager, 1),
                    source_scope,
                    Box::new(())
                )
                .is_ok()
        );
        let source = TransferRegistry::new(manager, source_scope, 2, Arc::clone(&dependencies));
        let destination =
            TransferRegistry::new(manager, destination_scope, 1, Arc::clone(&dependencies));
        source.insert(id(manager, 1), "parent").unwrap();
        source.insert(id(manager, 2), "child").unwrap();

        let error = source
            .transfer_group_to(&destination, &[id(manager, 1), id(manager, 2)])
            .unwrap_err();
        assert_eq!(error, ResourceTransferError::CapacityExceeded);
        assert_eq!(source.len(), 2);
        assert_eq!(destination.len(), 0);

        let destination =
            TransferRegistry::new(manager, destination_scope, 2, Arc::clone(&dependencies));
        let transfer = source
            .transfer_group_to(&destination, &[id(manager, 1), id(manager, 2)])
            .unwrap();
        assert_eq!(transfer.members.len(), 2);
        assert_eq!(transfer.receipt.resource(), id(manager, 1));
        assert_eq!(transfer.receipt.source_scope(), source_scope);
        assert_eq!(transfer.receipt.target_scope(), destination_scope);
        assert_eq!(source.len(), 0);
        assert_eq!(destination.len(), 2);
    }

    #[test]
    fn incomplete_group_transfer_keeps_source_unchanged() {
        let manager = ManagerId::new_unique();
        let scope = ResourceScopeId::new_unique();
        let dependencies = Arc::new(DependencyGraph::default());
        assert!(
            dependencies
                .add(id(manager, 2), scope, id(manager, 1), scope, Box::new(()))
                .is_ok()
        );
        let source = TransferRegistry::new(manager, scope, 2, Arc::clone(&dependencies));
        let destination = TransferRegistry::new(
            manager,
            ResourceScopeId::new_unique(),
            2,
            Arc::clone(&dependencies),
        );
        source.insert(id(manager, 1), "only member").unwrap();
        assert_eq!(
            source.transfer_group_to(&destination, &[id(manager, 1), id(manager, 2)]),
            Err(ResourceTransferError::IncompleteDependencyGroup)
        );
        assert_eq!(source.len(), 1);
        assert_eq!(destination.len(), 0);
    }

    #[test]
    fn cleanup_started_group_cannot_be_reopened_by_transfer() {
        let manager = ManagerId::new_unique();
        let scope = ResourceScopeId::new_unique();
        let dependencies = Arc::new(DependencyGraph::default());
        let resource = id(manager, 1);
        let source = TransferRegistry::new(manager, scope, 1, Arc::clone(&dependencies));
        let destination = TransferRegistry::new(
            manager,
            ResourceScopeId::new_unique(),
            1,
            Arc::clone(&dependencies),
        );
        source.insert(resource, "owner").unwrap();
        assert!(dependencies.try_start(resource));
        assert_eq!(
            source.transfer_group_to(&destination, &[resource]),
            Err(ResourceTransferError::CleanupStarted)
        );
        assert_eq!(source.len(), 1);
        assert_eq!(destination.len(), 0);
    }
}

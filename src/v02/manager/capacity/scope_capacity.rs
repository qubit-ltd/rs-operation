// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::CapacityError;
use super::CapacityPool;
use super::ManagerCapacityPools;
use super::PermitBundle;
use super::ResourceLabel;
use super::ResourceRegistration;
use super::ScopeCapacityKind;
use crate::v02::report::ScopeLimits;

/// The active capacity path for one operation, including all applicable
/// ancestors. Child scopes add local pools without pre-reserving their limits.
#[derive(Clone)]
pub(crate) struct ScopeCapacity {
    /// Effective limits inherited by this operation/resource scope.
    pub(in crate::v02::manager::capacity) limits: ScopeLimits,
    /// Pools for descendants, including every applicable ancestor.
    pub(in crate::v02::manager::capacity) child_operation_pools: Vec<CapacityPool>,
    /// Pools for registered resources, including every applicable ancestor.
    pub(in crate::v02::manager::capacity) resource_pools: Vec<CapacityPool>,
    /// Pools for resource-scope instances, including every applicable ancestor.
    pub(in crate::v02::manager::capacity) resource_scope_pools: Vec<CapacityPool>,
    /// Pools for effect records, including every applicable ancestor.
    pub(in crate::v02::manager::capacity) effect_record_pools: Vec<CapacityPool>,
}

impl ScopeCapacity {
    /// Creates root pools for a numeric scope identity.
    ///
    /// # Parameters
    ///
    /// - `manager`: Supplies the manager-wide coordinator.
    /// - `scope_id`: Identifies this scope's capacity pools.
    /// - `limits`: Sets the root scope's effective capacities.
    ///
    /// # Returns
    ///
    /// The root scope capacity hierarchy.
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
            child_operation_pools: vec![pool(limits.child_operations, ScopeCapacityKind::ChildOperations)],
            resource_pools: vec![pool(limits.resources, ScopeCapacityKind::Resources)],
            resource_scope_pools: vec![pool(limits.resource_scopes, ScopeCapacityKind::ResourceScopes)],
            effect_record_pools: vec![pool(limits.effect_records, ScopeCapacityKind::EffectRecords)],
        }
    }

    /// Creates root pools labeled by the stable operation identity.
    ///
    /// # Parameters
    ///
    /// - `manager`: Supplies the manager-wide coordinator.
    /// - `scope_id`: Identifies this operation's capacity pools.
    /// - `limits`: Sets the root scope's effective capacities.
    ///
    /// # Returns
    ///
    /// The root scope capacity hierarchy.
    pub(crate) fn root_for_operation(
        manager: &ManagerCapacityPools,
        scope_id: crate::v02::report::OperationId,
        limits: ScopeLimits,
    ) -> Self {
        Self::root_with_identity(manager, scope_id.raw(), limits)
    }

    /// Creates root pools using a caller-provided raw identity.
    ///
    /// # Parameters
    ///
    /// - `manager`: Supplies the manager-wide coordinator.
    /// - `scope_id`: Identifies this scope's capacity pools.
    /// - `limits`: Sets the root scope's effective capacities.
    ///
    /// # Returns
    ///
    /// The root scope capacity hierarchy.
    pub(crate) fn root_with_identity(manager: &ManagerCapacityPools, scope_id: u128, limits: ScopeLimits) -> Self {
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
            child_operation_pools: vec![pool(limits.child_operations, ScopeCapacityKind::ChildOperations)],
            resource_pools: vec![pool(limits.resources, ScopeCapacityKind::Resources)],
            resource_scope_pools: vec![pool(limits.resource_scopes, ScopeCapacityKind::ResourceScopes)],
            effect_record_pools: vec![pool(limits.effect_records, ScopeCapacityKind::EffectRecords)],
        }
    }

    /// Derives a child whose capacities cannot exceed this scope's effective
    /// limits.
    ///
    /// # Parameters
    ///
    /// - `scope_id`: Identifies the child scope's capacity pools.
    /// - `requested`: Sets capacities that are clamped to this scope's limits.
    ///
    /// # Returns
    ///
    /// The derived child capacity hierarchy.
    pub(crate) fn child(&self, scope_id: u64, requested: ScopeLimits) -> Self {
        self.child_with_identity(u128::from(scope_id), requested)
    }

    /// Derives a child labeled by its stable operation identity.
    ///
    /// # Parameters
    ///
    /// - `scope_id`: Identifies the child operation's capacity pools.
    /// - `requested`: Sets capacities that are clamped to this scope's limits.
    ///
    /// # Returns
    ///
    /// The derived child capacity hierarchy.
    pub(crate) fn child_for_operation(
        &self,
        scope_id: crate::v02::report::OperationId,
        requested: ScopeLimits,
    ) -> Self {
        self.child_with_identity(scope_id.raw(), requested)
    }

    /// Creates resource-registration pools bounded by inherited limits and
    /// local capacity.
    ///
    /// # Parameters
    ///
    /// - `scope_id`: Identifies the resource scope's capacity pools.
    /// - `capacity`: Sets the local resource capacity before ancestor clamping.
    ///
    /// # Returns
    ///
    /// The derived resource scope capacity hierarchy.
    pub(crate) fn resource_scope_for(&self, scope_id: crate::v02::report::ResourceScopeId, capacity: usize) -> Self {
        let limits = ScopeLimits {
            child_operations: self.limits.child_operations,
            resources: self.limits.resources.min(capacity),
            resource_scopes: self.limits.resource_scopes,
            effect_records: self.limits.effect_records,
        };
        self.child_with_identity(scope_id.raw(), limits)
    }

    /// Builds a descendant hierarchy with each requested limit clamped to its
    /// parent.
    ///
    /// # Parameters
    ///
    /// - `scope_id`: Identifies the descendant's capacity pools.
    /// - `requested`: Sets capacities that are clamped to the parent limits.
    ///
    /// # Returns
    ///
    /// The derived descendant capacity hierarchy.
    fn child_with_identity(&self, scope_id: u128, requested: ScopeLimits) -> Self {
        let limits = ScopeLimits {
            child_operations: self.limits.child_operations.min(requested.child_operations),
            resources: self.limits.resources.min(requested.resources),
            resource_scopes: self.limits.resource_scopes.min(requested.resource_scopes),
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
        child_operation_pools.push(pool(limits.child_operations, ScopeCapacityKind::ChildOperations));
        let mut resource_pools = self.resource_pools.clone();
        resource_pools.push(pool(limits.resources, ScopeCapacityKind::Resources));
        let mut resource_scope_pools = self.resource_scope_pools.clone();
        resource_scope_pools.push(pool(limits.resource_scopes, ScopeCapacityKind::ResourceScopes));
        let mut effect_record_pools = self.effect_record_pools.clone();
        effect_record_pools.push(pool(limits.effect_records, ScopeCapacityKind::EffectRecords));
        Self {
            limits,
            child_operation_pools,
            resource_pools,
            resource_scope_pools,
            effect_record_pools,
        }
    }

    /// Returns the effective limits after intersecting ancestor and requested
    /// limits.
    #[inline]
    pub(crate) fn effective_limits(&self) -> ScopeLimits {
        self.limits
    }

    /// Acquires descendant-operation capacity across all applicable ancestors.
    ///
    /// # Errors
    ///
    /// Returns an error if any pool in the descendant-operation hierarchy has
    /// no available capacity.
    pub(crate) fn child_operation_permits(&self) -> Result<PermitBundle, CapacityError> {
        PermitBundle::try_acquire(&self.child_operation_pools)
    }

    /// Returns the hierarchical pools used for child operation admission.
    pub(crate) fn child_operation_pool_plan(&self) -> Vec<CapacityPool> {
        self.child_operation_pools.clone()
    }

    /// Acquires a resource-registration permit across all applicable ancestors.
    ///
    /// # Errors
    ///
    /// Returns an error if any resource-registration pool has no available
    /// capacity.
    pub(crate) fn resource_registration(&self) -> Result<ResourceRegistration, CapacityError> {
        PermitBundle::try_acquire(&self.resource_pools).map(|permits| ResourceRegistration {
            permits: Arc::new(permits),
        })
    }

    /// Acquires one resource-scope instance permit across all applicable
    /// ancestors.
    ///
    /// # Errors
    ///
    /// Returns an error if any resource-scope pool has no available capacity.
    pub(crate) fn resource_scope_permits(&self) -> Result<PermitBundle, CapacityError> {
        PermitBundle::try_acquire(&self.resource_scope_pools)
    }

    /// Acquires effect-record capacity across all applicable ancestors.
    ///
    /// # Errors
    ///
    /// Returns an error if any effect-record pool has no available capacity.
    pub(crate) fn effect_record_permits(&self) -> Result<PermitBundle, CapacityError> {
        PermitBundle::try_acquire(&self.effect_record_pools)
    }

    /// Returns the minimum available descendant-operation capacity in the
    /// hierarchy.
    pub(crate) fn child_capacity_available(&self) -> usize {
        self.child_operation_pools
            .iter()
            .map(CapacityPool::available)
            .min()
            .unwrap_or(0)
    }

    /// Returns the minimum available resource-registration capacity in the
    /// hierarchy.
    pub(crate) fn resource_capacity_available(&self) -> usize {
        self.resource_pools
            .iter()
            .map(CapacityPool::available)
            .min()
            .unwrap_or(0)
    }
}

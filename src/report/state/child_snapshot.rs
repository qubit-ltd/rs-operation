// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::super::OperationId;
use super::OperationPhase;
use super::ResourcePhase;

/// Bounded child-operation snapshot; it reports recorded facts without
/// consuming results or proving facts beyond its fields.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::{
///     ChildSnapshot, OperationId, OperationPhase, ResourcePhase,
/// };
///
/// let snapshot = ChildSnapshot {
///     operation_id: OperationId::new(7),
///     phase: OperationPhase::Running,
///     resource_phase: ResourcePhase::Active,
///     stop_requested: false,
///     released: false,
/// };
/// assert_eq!(snapshot.phase, OperationPhase::Running);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildSnapshot {
    /// Child operation identity within its supervisor.
    pub operation_id: OperationId,
    /// Business result-delivery phase recorded for the child.
    pub phase: OperationPhase,
    /// Resource cleanup and release phase recorded for the child.
    pub resource_phase: ResourcePhase,
    /// Whether the supervisor accepted a stop request for the child.
    pub stop_requested: bool,
    /// Whether release has been confirmed by the owning domain.
    pub released: bool,
}

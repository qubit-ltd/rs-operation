// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::EffectFact;
use crate::report::DiagnosticSummary;
use crate::report::OperationId;
use crate::report::ResourceId;

/// Caller-declared side-effect fact; the framework does not independently
/// verify it.
///
/// # Examples
///
/// An operation can submit a caller-declared fact to its scope for inclusion
/// in the final settlement report:
///
/// ```
/// use qubit_operation::report::DiagnosticSummary;
/// use qubit_operation::report::EffectFact;
/// use qubit_operation::report::EffectRecord;
/// use qubit_operation::report::OperationId;
/// use qubit_operation::report::ResourceId;
/// use qubit_operation::scope::OperationScope;
/// use qubit_operation::scope::ScopeError;
///
/// fn report_effect(scope: &OperationScope) -> Result<(), ScopeError> {
///     let effect = EffectRecord {
///         operation_id: OperationId::new(7),
///         resource_id: Some(ResourceId::new(12)),
///         description: DiagnosticSummary::new("checkpoint persisted"),
///         fact: EffectFact::Known,
///     };
///     scope.record_effect(effect)
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EffectRecord {
    /// Operation that reported the effect.
    pub operation_id: OperationId,
    /// Related resource, when known.
    pub resource_id: Option<ResourceId>,
    /// Bounded effect description provided by the caller.
    pub description: DiagnosticSummary,
    /// Whether the caller considers the effect known or uncertain.
    pub fact: EffectFact,
}

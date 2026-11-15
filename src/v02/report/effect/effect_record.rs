// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::DiagnosticSummary;
use crate::v02::report::EffectFact;
use crate::v02::report::OperationId;
use crate::v02::report::ResourceId;

/// Bounded record describing a side effect associated with an operation.
///
/// # Examples
///
/// Inspect an effect record, including whether it concerns a managed resource:
///
/// ```
/// use qubit_operation::{EffectFact, EffectRecord, OperationId, ResourceId};
///
/// fn effect_details(
///     record: &EffectRecord,
/// ) -> (OperationId, Option<ResourceId>, EffectFact, &str) {
///     (
///         record.operation_id,
///         record.resource_id,
///         record.fact,
///         record.description.as_str(),
///     )
/// }
/// ```
#[derive(Clone, Debug)]
pub struct EffectRecord {
    /// Operation that produced or may have produced the effect.
    pub operation_id: OperationId,
    /// Related resource as `Some` if one is involved, or `None` otherwise.
    pub resource_id: Option<ResourceId>,
    /// Whether the effect is known or uncertain.
    pub fact: EffectFact,
    /// Short diagnostic describing the side effect.
    pub description: DiagnosticSummary,
}

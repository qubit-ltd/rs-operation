// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::SubmissionLease;
use super::SubmissionPermit;
use crate::v02::manager::capacity::ScopeCapacity;
use crate::v02::resource::ResourceScope;

/// The complete, reversible reservation made before tree registration.
pub(crate) struct SubmissionReservation {
    /// Executor permit consumed by the first dispatch or returned on rollback.
    pub(crate) execution: Box<dyn SubmissionPermit>,
    /// Lifecycle permits released at execution, settlement, and delivery
    /// points.
    pub(crate) lifecycle: Arc<dyn SubmissionLease>,
    /// Hierarchical capacity attached to the operation scope.
    pub(crate) capacity: Arc<ScopeCapacity>,
    /// Resource registry owned by the operation until scope completion.
    pub(crate) resource_scope: ResourceScope,
}

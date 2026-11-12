// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! State recorded for one registered child operation.

use std::sync::Arc;

use crate::protocol::ManagedOperationControl;
use crate::report::OperationId;
use crate::resource::ResourceUse;

use super::super::ChildRequirement;

/// Registration and settlement state retained for a child operation.
pub(super) struct Child {
    /// Identity captured from the managed child control.
    pub(in crate::scope) id: OperationId,
    /// Control used to request stop and observe business/release completion.
    pub(in crate::scope) control: Arc<dyn ManagedOperationControl>,
    /// Whether failure affects the parent operation result.
    pub(in crate::scope) requirement: ChildRequirement,
    /// Whether the reservation was committed and the child may start.
    pub(in crate::scope) committed: bool,
    /// Resource-use leases held until the child releases its resources.
    pub(in crate::scope) uses: Option<Vec<ResourceUse>>,
}

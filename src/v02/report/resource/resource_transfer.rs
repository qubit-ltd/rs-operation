// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use crate::v02::report::ResourceId;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::TransferReceipt;

/// Record of a dependency-complete resource group moved between scopes.
///
/// The transfer protocol creates this value only after all listed members have
/// moved. It records resource identities and the receiver's receipt; it does
/// not contain or extend the lifetime of the resource values.
///
/// # Examples
///
/// A transfer record contains the complete member identities and scopes:
///
/// ```
/// use qubit_operation::ResourceTransfer;
///
/// fn member_count(transfer: &ResourceTransfer) -> usize {
///     transfer.members.len()
/// }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct ResourceTransfer {
    /// Resources moved as one dependency-complete group.
    pub members: Arc<[ResourceId]>,
    /// Scope that relinquished the resources.
    pub source_scope: ResourceScopeId,
    /// Scope that accepted the resources.
    pub target_scope: ResourceScopeId,
    /// Proof minted by the receiving transfer protocol.
    pub receipt: TransferReceipt,
}

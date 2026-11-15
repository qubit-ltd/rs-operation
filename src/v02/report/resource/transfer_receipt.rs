// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::ResourceId;
use crate::v02::report::ResourceScopeId;

/// Proof that a resource transfer was accepted by a receiving scope.
///
/// Fields remain private so callers cannot forge transfer authorization.
///
/// # Examples
///
/// A successful transfer protocol supplies a receipt that callers can inspect:
///
/// ```
/// use qubit_operation::TransferReceipt;
/// use qubit_operation::ResourceScopeId;
///
/// fn destination_scope(receipt: &TransferReceipt) -> ResourceScopeId {
///     receipt.target_scope()
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransferReceipt {
    /// Identity of the resource whose ownership was transferred.
    resource: ResourceId,
    /// Scope that relinquished ownership.
    source_scope: ResourceScopeId,
    /// Scope that accepted ownership.
    target_scope: ResourceScopeId,
    /// Internal token used to validate this receipt's provenance.
    nonce: u64,
}

impl TransferReceipt {
    /// Mints a receipt inside the transfer protocol after validation succeeds.
    ///
    /// # Parameters
    ///
    /// - `resource`: Identity of the resource whose ownership moved.
    /// - `source_scope`: Scope that relinquished the resource.
    /// - `target_scope`: Scope that accepted the resource.
    /// - `nonce`: Internal provenance token for validating the receipt.
    ///
    /// # Returns
    ///
    /// A receipt proving the transfer protocol accepted the resource.
    #[allow(dead_code)]
    pub(crate) const fn new(
        resource: ResourceId,
        source_scope: ResourceScopeId,
        target_scope: ResourceScopeId,
        nonce: u64,
    ) -> Self {
        Self {
            resource,
            source_scope,
            target_scope,
            nonce,
        }
    }
    /// Returns the identity of the transferred resource.
    ///
    /// # Returns
    ///
    /// The resource identity recorded by the transfer protocol.
    pub const fn resource(&self) -> ResourceId {
        self.resource
    }
    /// Returns the scope from which ownership moved.
    ///
    /// # Returns
    ///
    /// The source scope recorded when the transfer was accepted.
    pub const fn source_scope(&self) -> ResourceScopeId {
        self.source_scope
    }
    /// Returns the receiving scope that accepted ownership.
    ///
    /// # Returns
    ///
    /// The target scope recorded when the transfer was accepted.
    pub const fn target_scope(&self) -> ResourceScopeId {
        self.target_scope
    }
    /// Returns the internal nonce used to validate receipt provenance.
    ///
    /// # Returns
    ///
    /// The internal transfer provenance token.
    #[allow(dead_code)]
    pub(crate) const fn nonce(&self) -> u64 {
        self.nonce
    }
}

#[cfg(test)]
mod tests {
    use super::TransferReceipt;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ResourceId;
    use crate::v02::report::ResourceScopeId;

    #[test]
    fn test_receipt_retains_provenance_token_and_transfer_endpoints() {
        let resource = ResourceId::new(ManagerId::from_raw(1), 2, 3);
        let source = ResourceScopeId::from_raw(4);
        let target = ResourceScopeId::from_raw(5);
        let receipt = TransferReceipt::new(resource, source, target, 99);
        assert_eq!(receipt.resource(), resource);
        assert_eq!(receipt.source_scope(), source);
        assert_eq!(receipt.target_scope(), target);
        assert_eq!(receipt.nonce(), 99);
    }
}

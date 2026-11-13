// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::ResourceId;

/// Evidence that a cleanup owner was accepted by a new recovery owner.
///
/// Callers can inspect receipts but cannot construct one before a receiving
/// registry accepts responsibility.
///
/// ```compile_fail
/// use qubit_operation::report::OperationId;
/// use qubit_operation::report::ResourceId;
/// use qubit_operation::report::TransferReceipt;
///
/// let resource = ResourceId::new(OperationId::new(1), 2, 3);
/// let _ = TransferReceipt { resource, generation: 3 };
/// ```
///
/// A receiver registry returns a receipt from
/// [`crate::resource::ResourceRegistry::accept_cleanup_transfer`]. The source
/// registry then uses that proof to record the accepted transfer.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::ResourceId;
/// use qubit_operation::report::TransferReceipt;
/// use qubit_operation::resource::ResourceRegistry;
///
/// fn confirm_source_transfer(
///     source_registry: &ResourceRegistry,
///     resource: ResourceId,
///     receipt: TransferReceipt,
/// ) {
///     assert!(source_registry.confirm_transferred(resource, receipt).is_ok());
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[must_use = "pass the acceptance proof to the source registry"]
pub struct TransferReceipt {
    /// Source resource registration whose ownership was transferred.
    resource: ResourceId,
    /// Source generation accepted by the receiving registry.
    generation: u32,
    /// Resource identity assigned to the accepted owner by the receiver.
    receiver: ResourceId,
}

impl TransferReceipt {
    /// Create a receipt only after the receiving registry accepted
    /// responsibility.
    ///
    /// # Parameters
    ///
    /// - `resource`: source registration whose owner was transferred.
    /// - `receiver`: receiving registration that accepted the owner.
    ///
    /// # Returns
    ///
    /// A receipt binding both registrations and the source generation.
    #[inline]
    pub(crate) const fn accepted_by_registry(resource: ResourceId, receiver: ResourceId) -> Self {
        Self {
            resource,
            generation: resource.generation,
            receiver,
        }
    }

    /// Return the resource whose responsibility was transferred.
    ///
    /// # Returns
    ///
    /// The source resource registration recorded by the receipt.
    #[inline]
    pub const fn resource(self) -> ResourceId {
        self.resource
    }

    /// Return the accepted resource generation.
    ///
    /// # Returns
    ///
    /// The generation from the source registration when accepted.
    #[inline]
    pub const fn generation(self) -> u32 {
        self.generation
    }

    /// Resource identity assigned by the registry that accepted responsibility.
    ///
    /// # Returns
    ///
    /// The receiver-side registration paired with the source resource.
    #[inline]
    pub const fn receiver(self) -> ResourceId {
        self.receiver
    }
}

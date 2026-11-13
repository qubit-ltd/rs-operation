// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use super::ResourceId;

/// Receiver-issued evidence that an accepted transferred resource was released.
///
/// A receiver obtains this proof from
/// [`crate::resource::ResourceRegistry::confirm_released_with_receipt`]; the
/// source registry consumes it to settle the transferred tombstone.
///
/// # Examples
///
/// ```
/// use qubit_operation::report::ReleaseReceipt;
/// use qubit_operation::report::ResourceId;
/// use qubit_operation::resource::ResourceRegistry;
///
/// // The receipt is returned by the receiver registry after cleanup completes.
/// fn settle_source(
///     source_registry: &ResourceRegistry,
///     source: ResourceId,
///     receipt: ReleaseReceipt,
/// ) {
///     assert!(source_registry
///         .confirm_transferred_released(source, receipt)
///         .is_ok());
/// }
/// ```
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[must_use = "use the release proof to settle its source resource tombstone"]
pub struct ReleaseReceipt {
    /// Source registration whose transferred tombstone this proof settles.
    source: ResourceId,
    /// Receiver registration whose release was confirmed.
    receiver: ResourceId,
}

impl ReleaseReceipt {
    /// Create proof after the receiver registry has confirmed resource release.
    ///
    /// # Parameters
    ///
    /// - `source`: source registration whose transferred tombstone will be
    ///   settled.
    /// - `receiver`: receiver registration whose release was confirmed.
    ///
    /// # Returns
    ///
    /// A receipt binding the source tombstone to the receiver's release
    /// confirmation.
    #[inline]
    pub(crate) const fn confirmed(source: ResourceId, receiver: ResourceId) -> Self {
        Self { source, receiver }
    }

    /// Original resource identity whose transferred tombstone can be settled.
    ///
    /// # Returns
    ///
    /// The source registration represented by this receipt.
    #[inline]
    pub const fn source(self) -> ResourceId {
        self.source
    }

    /// Receiver resource identity whose release was confirmed.
    ///
    /// # Returns
    ///
    /// The receiver registration represented by this receipt.
    #[inline]
    pub const fn receiver(self) -> ResourceId {
        self.receiver
    }
}

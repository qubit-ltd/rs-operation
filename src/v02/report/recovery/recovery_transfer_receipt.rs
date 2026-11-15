// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::RecoveryId;
use crate::v02::report::RecoveryStoreId;

/// Proof that a recovery record was accepted by a recovery store.
///
/// Receipts are returned as part of
/// [`RecoveryTransfer`](crate::RecoveryTransfer) after a store accepts
/// ownership; callers can inspect the record and store identities without being
/// able to construct a receipt themselves.
///
/// # Examples
///
/// ```
/// use qubit_operation::{RecoveryStoreId, RecoveryTransfer};
///
/// fn receiving_store(transfer: &RecoveryTransfer) -> RecoveryStoreId {
///     transfer.receipt.receiver()
/// }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct RecoveryTransferReceipt {
    /// Recovery record moved to the receiving store.
    recovery: RecoveryId,
    /// Store that accepted the recovery record.
    receiver: RecoveryStoreId,
    /// Internal nonce used to validate receipt provenance.
    nonce: u64,
}

impl RecoveryTransferReceipt {
    /// Mints a receipt after a recovery store accepts the record.
    ///
    /// # Parameters
    ///
    /// - `recovery`: Identity of the recovery record accepted by the store.
    /// - `receiver`: Identity of the store accepting the record.
    /// - `nonce`: Internal value distinguishing this transfer receipt.
    ///
    /// # Returns
    ///
    /// A receipt bound to the accepted recovery record, store, and nonce.
    #[allow(dead_code)]
    pub(crate) const fn new(recovery: RecoveryId, receiver: RecoveryStoreId, nonce: u64) -> Self {
        Self {
            recovery,
            receiver,
            nonce,
        }
    }
    /// Returns the recovery record identity bound to this receipt.
    ///
    /// # Parameters
    ///
    /// - `self`: Receipt whose recovery identity is read.
    ///
    /// # Returns
    ///
    /// The identity of the recovery record accepted by the receiving store.
    #[inline]
    pub const fn recovery(&self) -> RecoveryId {
        self.recovery
    }
    /// Returns the receiving store identity bound to this receipt.
    ///
    /// # Parameters
    ///
    /// - `self`: Receipt whose receiving store identity is read.
    ///
    /// # Returns
    ///
    /// The identity of the store that accepted the recovery record.
    #[inline]
    pub const fn receiver(&self) -> RecoveryStoreId {
        self.receiver
    }
}

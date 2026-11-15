// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::report::RecoveryId;
use crate::v02::report::RecoveryStoreId;
use crate::v02::report::RecoveryTransferReceipt;

/// Recovery record and receipt returned when a store accepts its ownership.
///
/// A [`RecoveryHandle::transfer_to`](crate::RecoveryHandle::transfer_to) call
/// produces this report after the receiving store accepts the record.
///
/// # Examples
///
/// ```
/// use qubit_operation::{RecoveryStoreId, RecoveryTransfer};
///
/// // The value is returned by RecoveryHandle::transfer_to.
/// fn receiving_store(transfer: &RecoveryTransfer) -> RecoveryStoreId {
///     transfer.receiver
/// }
/// ```
#[derive(Debug, Eq, PartialEq)]
pub struct RecoveryTransfer {
    /// Identity of the recovery record.
    pub recovery: RecoveryId,
    /// Identity of the receiving recovery store.
    pub receiver: RecoveryStoreId,
    /// Receipt proving that the store accepted this recovery record.
    pub receipt: RecoveryTransferReceipt,
}

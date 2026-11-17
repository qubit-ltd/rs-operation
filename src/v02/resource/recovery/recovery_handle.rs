// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::fmt;
use std::sync::Arc;

use crate::v02::report::RecoveryId;
use crate::v02::report::RecoverySnapshot;
use crate::v02::report::RecoveryTransfer;
use crate::v02::report::RecoveryTransferError;
use crate::v02::resource::recovery::RecoveryAttempt;
use crate::v02::resource::recovery::RecoveryCore;
use crate::v02::resource::recovery::RecoveryStore;
use crate::v02::resource::recovery::RecoveryStoreError;

/// Shared observation and retry capability for one retained recovery group.
///
/// Cloning a handle shares the same recovery state; it does not create an
/// independent retry. Keep at least one handle or transfer the record to a
/// [`RecoveryStore`] while cleanup remains unresolved. Handles are supplied in
/// resource-scope and settlement reports.
///
/// # Examples
///
/// ```
/// use qubit_operation::ResourceScopeReport;
/// use qubit_operation::RecoveryStore;
///
/// fn transfer_first_recovery(report: &ResourceScopeReport, destination: &RecoveryStore) {
///     let Some(handle) = report.recoveries.first() else {
///         return;
///     };
///     let _id = handle.id();
///     let _snapshot = handle.snapshot();
///     match handle.transfer_to(destination) {
///         Ok(transfer) => {
///             let _accepted_recovery = transfer.receipt.recovery();
///         }
///         Err(error) => {
///             let _transfer_error = error;
///         }
///     }
/// }
/// ```
#[must_use = "retain this handle or transfer or retry unresolved cleanup responsibility"]
#[derive(Clone)]
pub struct RecoveryHandle {
    /// Shared recovery state observed and controlled by this handle.
    pub(crate) core: Arc<RecoveryCore>,
}

impl RecoveryHandle {
    /// Wraps an existing shared recovery group for report delivery.
    ///
    /// # Parameters
    ///
    /// - `core`: Recovery state shared by this handle and any clones.
    ///
    /// # Returns
    ///
    /// A handle that can inspect, retry or transfer the recovery group.
    pub(crate) fn new(core: Arc<RecoveryCore>) -> Self {
        Self { core }
    }

    /// Returns the stable identity of this shared recovery group.
    ///
    /// # Parameters
    ///
    /// - `self`: The handle whose recovery identity is queried.
    ///
    /// # Returns
    ///
    /// The group's unique recovery ID.
    pub fn id(&self) -> RecoveryId {
        self.core.id()
    }

    /// Returns a point-in-time snapshot of this group's recovery state.
    ///
    /// The snapshot retains at most 64 recent failures in oldest-to-newest
    /// order. `failures_total` counts all failure events since the last
    /// confirmed cleanup, saturating at `usize::MAX`; `failures_truncated`
    /// indicates that older entries were evicted. Successful cleanup clears
    /// the history and resets both fields. Applications that need a complete
    /// history should emit each failure to their own logging system.
    ///
    /// # Parameters
    ///
    /// - `self`: The handle whose recovery state is observed.
    ///
    /// # Returns
    ///
    /// The current phase, resources, attempt count, recent failures, and
    /// failure-history metadata.
    pub fn snapshot(&self) -> RecoverySnapshot {
        self.core.snapshot()
    }

    /// Starts another cleanup attempt for this recovery group.
    ///
    /// Only one attempt can run at a time. The returned attempt represents the
    /// asynchronous cleanup; errors leave the caller with this recovery handle.
    ///
    /// # Parameters
    ///
    /// - `self`: The handle identifying the unresolved recovery group.
    ///
    /// # Returns
    ///
    /// A handle to the asynchronous retry attempt.
    ///
    /// # Errors
    ///
    /// Returns [`crate::v02::report::RetryError`] when an attempt is already
    /// running, the group is resolved, capacity is exhausted, or no executor
    /// can accept the retry.
    pub fn try_retry(&self) -> Result<RecoveryAttempt, crate::v02::report::RetryError> {
        self.core.try_retry()
    }

    /// Transfers this recovery group to a destination store.
    ///
    /// After acceptance, that store permanently owns the retry route for the
    /// group, including across failed retries. A later transfer attempt is
    /// rejected. A failed initial transfer does not produce a receipt and
    /// leaves this handle available to the caller.
    ///
    /// # Parameters
    ///
    /// - `self`: The handle for the recovery group being transferred.
    /// - `destination`: Store that will accept and retain the group.
    ///
    /// # Returns
    ///
    /// A recovery transfer record with a receipt after successful acceptance.
    ///
    /// # Errors
    ///
    /// Returns [`RecoveryTransferError`] when the store is closed, full, the
    /// group was already accepted by any store, or the group is invalid.
    pub fn transfer_to(&self, destination: &RecoveryStore) -> Result<RecoveryTransfer, RecoveryTransferError> {
        destination
            .accept(Arc::clone(&self.core))
            .map_err(|(error, _owner)| match error {
                RecoveryStoreError::Closed => RecoveryTransferError::Closed,
                RecoveryStoreError::GroupCapacity | RecoveryStoreError::ResourceCapacity => {
                    RecoveryTransferError::CapacityExceeded
                }
                RecoveryStoreError::AlreadyTransferred => RecoveryTransferError::AlreadyTransferred,
                RecoveryStoreError::InvalidGroup => RecoveryTransferError::InvalidGroup,
            })
    }
}

impl fmt::Debug for RecoveryHandle {
    /// Formats the handle using its stable recovery identity.
    ///
    /// # Parameters
    ///
    /// - `self`: The handle being formatted.
    /// - `f`: Formatter that receives the debug tuple.
    ///
    /// # Returns
    ///
    /// The formatter's result.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("RecoveryHandle").field(&self.id()).finish()
    }
}

impl PartialEq for RecoveryHandle {
    /// Compares handles by recovery identity, including clones of one group.
    ///
    /// # Parameters
    ///
    /// - `self`: First handle to compare.
    /// - `other`: Second handle to compare.
    ///
    /// # Returns
    ///
    /// `true` when both handles refer to the same recovery ID.
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl Eq for RecoveryHandle {}

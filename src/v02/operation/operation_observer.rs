// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::time::Duration;

use super::async_operation_observer::AsyncOperationObserver;
use super::internal::OperationCore;
use crate::v02::report::ObservationError;
use crate::v02::report::OperationId;
use crate::v02::report::OperationSnapshot;
use crate::v02::report::SettlementReport;
use crate::v02::report::SettlementWait;

/// Synchronous, untyped read-only lifecycle observer.
///
/// The observer can inspect execution and settlement state without owning or
/// taking the operation's typed outcome.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationManager;
/// use qubit_operation::{ManagerOptions, OperationOptions};
///
/// let manager = OperationManager::new(ManagerOptions::default()).unwrap();
/// let operation = manager
///     .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
///     .unwrap();
/// let observer = operation.observer();
/// assert_eq!(observer.id(), operation.id());
/// ```
#[must_use = "retain the observer to inspect operation lifecycle state"]
#[derive(Clone)]
pub struct OperationObserver {
    /// Shared lifecycle core retained for read-only operation observation.
    pub(in crate::v02::operation) core: Arc<OperationCore>,
}

impl OperationObserver {
    /// Returns this operation's stable ID.
    ///
    /// # Returns
    ///
    /// The ID shared by the operation and all its observer views.
    #[must_use = "use the operation ID to correlate this observer with its source"]
    #[inline]
    pub fn id(&self) -> OperationId {
        self.core.id()
    }

    /// Returns the bounded diagnostics reported by external protocol ports.
    /// Faults are ordered Release then Settlement and do not alter the
    /// operation outcome.
    pub fn external_protocol_faults(&self) -> Arc<[crate::v02::report::ExternalProtocolFault]> {
        self.core.external_protocol_faults()
    }

    /// Returns the latest untyped lifecycle snapshot.
    ///
    /// # Returns
    ///
    /// A snapshot of the operation's current execution and settlement state.
    #[must_use = "use the snapshot to inspect the latest operation state"]
    #[inline]
    pub fn snapshot(&self) -> OperationSnapshot {
        self.core.snapshot()
    }

    /// Blocks until the stable settlement report is published.
    ///
    /// This observer does not own the typed outcome, but any handle, result,
    /// error, clone, or other owner can retain a
    /// [`crate::v02::resource::ResourceRef`] needed for settlement. Release
    /// those references before waiting. `WouldDeadlock` only detects this
    /// operation's blocking worker, not every resource wait cycle.
    ///
    /// # Returns
    ///
    /// A shared settlement report after settlement completes.
    ///
    /// # Errors
    ///
    /// Returns `WouldDeadlock` if called from a blocking operation worker.
    pub fn wait_settlement(&self) -> Result<Arc<SettlementReport>, ObservationError> {
        if self.core.is_blocking_worker() {
            return Err(ObservationError::WouldDeadlock);
        }
        Ok(self.core.wait_settlement_blocking())
    }

    /// Waits for settlement for at most `timeout`, returning the current
    /// snapshot on timeout.
    ///
    /// Other owners, including an unconsumed outcome and references inside
    /// success or error values, can keep settlement pending. A timeout only
    /// describes this observation; it does not prove release.
    ///
    /// # Parameters
    ///
    /// - `timeout`: Maximum duration to wait for settlement publication.
    ///
    /// # Returns
    ///
    /// A completed report or the latest snapshot if the timeout expires.
    ///
    /// # Errors
    ///
    /// Returns `WouldDeadlock` if called from a blocking operation worker.
    pub fn wait_settlement_for(&self, timeout: Duration) -> Result<SettlementWait, ObservationError> {
        if self.core.is_blocking_worker() {
            return Err(ObservationError::WouldDeadlock);
        }
        Ok(self.core.wait_settlement_for(timeout))
    }

    /// Creates the async view of this read-only observer.
    ///
    /// # Returns
    ///
    /// An async observer retaining the same operation lifecycle core.
    #[must_use = "retain the asynchronous observer to inspect operation lifecycle state"]
    #[inline]
    pub fn as_async(&self) -> AsyncOperationObserver {
        AsyncOperationObserver {
            core: Arc::clone(&self.core),
        }
    }
}

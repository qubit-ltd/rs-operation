// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::time::Duration;

use super::async_operation_handle::AsyncOperationHandle;
use super::internal::HandleParts;
use super::internal::OperationCore;
use super::internal::OutcomeSlot;
use super::internal::StopControl;
use super::operation_observer::OperationObserver;
use crate::v02::report::ExecutionPhase;
use crate::v02::report::ObservationError;
use crate::v02::report::OperationId;
use crate::v02::report::OperationOutcome;
use crate::v02::report::OperationSnapshot;
use crate::v02::report::OutcomeError;
use crate::v02::report::SettlementReport;
use crate::v02::report::SettlementWait;
use crate::v02::report::StopReason;
use crate::v02::report::StopRequestStatus;

/// The synchronous one-shot receiver for an operation outcome.
///
/// Dropping the handle discards its typed outcome and requests user
/// cancellation if the operation has not finished. Waiting for settlement is
/// separate from receiving the outcome.
///
/// # Type Parameters
///
/// - `T`: Successful value produced by the operation.
/// - `E`: Error value produced by the operation.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationManager;
/// use qubit_operation::{ManagerOptions, OperationOptions};
///
/// let manager = OperationManager::new(ManagerOptions::default()).unwrap();
/// let operation = manager
///     .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(42usize))
///     .unwrap();
/// let _operation_id = operation.id();
/// ```
#[must_use = "retain the handle to receive or discard the operation outcome"]
pub struct OperationHandle<T: Send + 'static, E: Send + 'static> {
    /// Unique typed receiver parts and lifecycle observers for this operation.
    pub(in crate::v02::operation) parts: Option<HandleParts<T, E>>,
}

impl<T: Send + 'static, E: Send + 'static> OperationHandle<T, E> {
    /// Creates a synchronous handle from the operation core, outcome slot, and
    /// stop control.
    ///
    /// # Parameters
    ///
    /// - `core`: Shared lifecycle state for the operation.
    /// - `slot`: One-shot typed outcome storage.
    /// - `stop`: Control used to propagate accepted stop requests.
    ///
    /// # Returns
    ///
    /// The unique synchronous receiver for this operation.
    #[inline]
    pub(crate) fn new(core: Arc<OperationCore>, slot: Arc<OutcomeSlot<T, E>>, stop: Arc<dyn StopControl>) -> Self {
        Self {
            parts: Some(HandleParts { core, slot, stop }),
        }
    }

    /// Returns this operation's stable ID.
    ///
    /// # Returns
    ///
    /// The ID shared by this handle and its read-only observers.
    #[must_use = "use the operation ID to correlate this handle with its source"]
    #[inline]
    pub fn id(&self) -> OperationId {
        self.parts().core.id()
    }

    /// Requests the first stop reason accepted by this operation.
    ///
    /// # Parameters
    ///
    /// - `reason`: Reason to record if no stop request has been accepted.
    ///
    /// # Returns
    ///
    /// Whether the reason was accepted, already requested, or arrived after
    /// completion.
    #[must_use = "check whether the stop request was accepted"]
    pub fn request_stop(&self, reason: StopReason) -> StopRequestStatus {
        let parts = self.parts();
        if parts.core.execution() == ExecutionPhase::Finished {
            return StopRequestStatus::AlreadyFinished;
        }
        if parts.slot.stop_report().is_some() {
            return StopRequestStatus::AlreadyRequested;
        }
        if parts.slot.request_stop(reason) {
            parts.stop.request_stop(reason);
            StopRequestStatus::Accepted
        } else if parts.slot.stop_report().is_some() {
            StopRequestStatus::AlreadyRequested
        } else {
            StopRequestStatus::AlreadyFinished
        }
    }

    /// Creates an untyped read-only view of this operation.
    ///
    /// # Returns
    ///
    /// An observer that can read lifecycle state without taking the typed
    /// outcome.
    #[must_use = "retain the observer to inspect operation lifecycle state"]
    #[inline]
    pub fn observer(&self) -> OperationObserver {
        OperationObserver {
            core: Arc::clone(&self.parts().core),
        }
    }

    /// Returns the latest untyped lifecycle snapshot.
    ///
    /// # Returns
    ///
    /// A snapshot of the operation's latest execution and settlement state.
    #[must_use = "use the snapshot to inspect the latest operation state"]
    #[inline]
    pub fn snapshot(&self) -> OperationSnapshot {
        self.parts().core.snapshot()
    }

    /// Blocks until the unique typed outcome is available and takes it.
    ///
    /// # Returns
    ///
    /// The operation outcome, removing it from this one-shot handle.
    ///
    /// # Errors
    ///
    /// Returns `AlreadyTaken` if the outcome was previously taken or discarded.
    pub fn wait_outcome(&mut self) -> Result<OperationOutcome<T, E>, OutcomeError> {
        self.parts().slot.take_blocking()
    }

    /// Takes the typed outcome if it is already available.
    ///
    /// # Returns
    ///
    /// `Some` with the one-shot outcome when ready, otherwise `None`.
    ///
    /// # Errors
    ///
    /// Returns `AlreadyTaken` if the outcome was previously taken or discarded.
    pub fn try_take_outcome(&mut self) -> Result<Option<OperationOutcome<T, E>>, OutcomeError> {
        self.parts().slot.take_if_ready()
    }

    /// Blocks until the stable settlement report is published.
    ///
    /// # Returns
    ///
    /// A shared settlement report.
    ///
    /// # Errors
    ///
    /// Returns `WouldDeadlock` when called from a blocking operation worker.
    pub fn wait_settlement(&self) -> Result<Arc<SettlementReport>, ObservationError> {
        let core = &self.parts().core;
        if core.is_blocking_worker() {
            return Err(ObservationError::WouldDeadlock);
        }
        Ok(core.wait_settlement_blocking())
    }

    /// Waits for settlement for at most `timeout`, returning the current
    /// snapshot on timeout.
    ///
    /// # Parameters
    ///
    /// - `timeout`: Maximum time to wait for settlement publication.
    ///
    /// # Returns
    ///
    /// A completed report or the latest snapshot when the timeout expires.
    ///
    /// # Errors
    ///
    /// Returns `WouldDeadlock` when called from a blocking operation worker.
    pub fn wait_settlement_for(&self, timeout: Duration) -> Result<SettlementWait, ObservationError> {
        let core = &self.parts().core;
        if core.is_blocking_worker() {
            return Err(ObservationError::WouldDeadlock);
        }
        Ok(core.wait_settlement_for(timeout))
    }

    /// Moves the unique outcome receiver into its async view without stopping
    /// the operation.
    ///
    /// # Returns
    ///
    /// An async handle for the same unique outcome receiver.
    #[must_use = "retain the asynchronous handle to receive the operation outcome"]
    #[inline]
    pub fn into_async(mut self) -> AsyncOperationHandle<T, E> {
        AsyncOperationHandle {
            parts: self.parts.take(),
        }
    }

    /// Discards the typed outcome and returns a read-only observer without
    /// requesting stop.
    ///
    /// # Returns
    ///
    /// An observer retaining the operation lifecycle after the typed outcome is
    /// discarded.
    #[must_use = "retain the observer to inspect operation lifecycle state"]
    #[inline]
    pub fn into_observer(mut self) -> OperationObserver {
        let parts = self.parts.take().expect("handle parts are present");
        parts.slot.discard();
        OperationObserver { core: parts.core }
    }

    /// Returns the internal handle parts while this receiver remains active.
    pub(in crate::v02::operation) fn parts(&self) -> &HandleParts<T, E> {
        self.parts.as_ref().expect("handle parts are present")
    }
}

impl<T: Send + 'static, E: Send + 'static> Drop for OperationHandle<T, E> {
    /// Requests cancellation when needed and discards an unclaimed outcome.
    ///
    /// # Parameters
    ///
    /// - `self`: The synchronous receiver being dropped.
    fn drop(&mut self) {
        let Some(parts) = self.parts.take() else {
            return;
        };
        if parts.core.execution() != ExecutionPhase::Finished && parts.slot.request_stop(StopReason::UserCancelled) {
            parts.stop.request_stop(StopReason::UserCancelled);
        }
        parts.slot.discard();
    }
}

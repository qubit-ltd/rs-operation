// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::async_operation_observer::AsyncOperationObserver;
use super::internal::HandleParts;
use super::internal::OperationCore;
use super::internal::OutcomeSlot;
use super::internal::StopControl;
use super::operation_handle::OperationHandle;
use crate::v02::report::ExecutionPhase;
use crate::v02::report::OperationId;
use crate::v02::report::OperationOutcome;
use crate::v02::report::OperationSnapshot;
use crate::v02::report::OutcomeError;
use crate::v02::report::SettlementReport;
use crate::v02::report::StopReason;
use crate::v02::report::StopRequestStatus;

/// The asynchronous one-shot receiver for an operation outcome.
///
/// Dropping the handle discards its typed outcome and requests user
/// cancellation if the operation has not finished. Waiting for settlement is a
/// separate async operation.
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
///     .unwrap()
///     .into_async();
/// let _operation_id = operation.id();
/// ```
#[must_use = "retain the handle to receive or discard the operation outcome"]
pub struct AsyncOperationHandle<T: Send + 'static, E: Send + 'static> {
    /// Unique typed receiver parts and lifecycle observers for this operation.
    pub(in crate::v02::operation) parts: Option<HandleParts<T, E>>,
}

impl<T: Send + 'static, E: Send + 'static> AsyncOperationHandle<T, E> {
    /// Creates an asynchronous handle from the operation core, outcome slot,
    /// and stop control.
    ///
    /// # Parameters
    ///
    /// - `core`: Shared lifecycle state for the operation.
    /// - `slot`: One-shot typed outcome storage.
    /// - `stop`: Control used to propagate accepted stop requests.
    ///
    /// # Returns
    ///
    /// The unique asynchronous receiver for this operation.
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
    #[must_use]
    #[inline]
    pub fn id(&self) -> OperationId {
        self.parts().core.id()
    }

    /// Returns the bounded diagnostics reported by external protocol ports.
    /// Faults are ordered Release then Settlement and do not alter the operation outcome.
    pub fn external_protocol_faults(&self) -> Arc<[crate::v02::report::ExternalProtocolFault]> {
        self.parts().core.external_protocol_faults()
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
    #[must_use]
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
    /// An async observer that can read lifecycle state without taking the typed
    /// outcome.
    #[must_use = "retain the observer to inspect operation lifecycle state"]
    #[inline]
    pub fn observer(&self) -> AsyncOperationObserver {
        AsyncOperationObserver {
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

    /// Asynchronously waits for and takes the unique typed outcome.
    ///
    /// # Returns
    ///
    /// The operation outcome, removing it from this one-shot handle.
    ///
    /// # Errors
    ///
    /// Returns `AlreadyTaken` if the outcome was previously taken or discarded.
    pub async fn wait_outcome(&mut self) -> Result<OperationOutcome<T, E>, OutcomeError> {
        self.parts().slot.take_async().await
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

    /// Asynchronously waits for the immutable settlement report.
    ///
    /// An unconsumed outcome can retain a [`crate::v02::resource::ResourceRef`]
    /// and keep settlement pending. Consume the outcome, then drop references
    /// held by either its success or error value and their clones before
    /// awaiting. See [`crate::v02::operation::OperationHandle::wait_settlement`]
    /// for a synchronous example of this ownership rule.
    ///
    /// # Returns
    ///
    /// A shared settlement report after all settlement work is published.
    #[must_use]
    pub async fn wait_settlement(&self) -> Arc<SettlementReport> {
        self.parts().core.wait_settlement_async().await
    }

    /// Moves the unique outcome receiver into its sync view without stopping
    /// the operation.
    ///
    /// # Returns
    ///
    /// A synchronous handle for the same unique outcome receiver.
    #[must_use = "retain the synchronous handle to receive the operation outcome"]
    #[inline]
    pub fn into_sync(mut self) -> OperationHandle<T, E> {
        OperationHandle {
            parts: self.parts.take(),
        }
    }

    /// Discards the typed outcome and returns a read-only observer without
    /// requesting stop.
    ///
    /// # Returns
    ///
    /// An async observer retaining operation lifecycle after the typed outcome
    /// is discarded.
    #[must_use = "retain the observer to inspect operation lifecycle state"]
    #[inline]
    pub fn into_observer(mut self) -> AsyncOperationObserver {
        let parts = self.parts.take().expect("handle parts are present");
        parts.slot.discard();
        AsyncOperationObserver { core: parts.core }
    }

    /// Returns the internal handle parts while this receiver remains active.
    pub(in crate::v02::operation) fn parts(&self) -> &HandleParts<T, E> {
        self.parts.as_ref().expect("handle parts are present")
    }
}

impl<T: Send + 'static, E: Send + 'static> Drop for AsyncOperationHandle<T, E> {
    /// Requests cancellation when needed and discards an unclaimed outcome.
    ///
    /// # Parameters
    ///
    /// - `self`: The asynchronous receiver being dropped.
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

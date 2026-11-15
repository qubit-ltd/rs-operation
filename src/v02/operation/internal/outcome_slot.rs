// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;

use super::helpers::lock;
use super::operation_core::OperationCore;
use super::outcome_delivery::OutcomeDelivery;
use crate::v02::internal::state::StateCell;
use crate::v02::operation::internal::DELIVERY_DISCARDED;
use crate::v02::operation::internal::DELIVERY_PENDING;
use crate::v02::operation::internal::DELIVERY_TAKEN;
use crate::v02::report::OperationOutcome;
use crate::v02::report::OutcomeError;
use crate::v02::report::StopReason;
use crate::v02::report::StopReport;

/// One-shot callback invoked when an outcome is delivered or discarded.
type DeliveryHook = Box<dyn FnOnce(OutcomeDelivery) + Send + 'static>;

/// The one typed outcome cell for an operation.
///
/// This is an adapter over T02's [`StateCell`], not a second result state
/// machine. It mirrors only the untyped availability bit into [`OperationCore`]
/// so observers never retain `T` or `E`.
///
/// # Type Parameters
///
/// - `T`: Successful outcome value stored by the operation.
/// - `E`: Failure value stored by the operation.
pub(crate) struct OutcomeSlot<T, E> {
    /// Shared lifecycle state for this operation.
    core: Arc<OperationCore>,
    /// Typed outcome and stop state owned by the result cell.
    cell: StateCell<T, E>,
    /// Atomic marker recording whether the outcome was delivered or discarded.
    delivery: AtomicU8,
    /// One-shot callback for returning resources after outcome delivery.
    delivery_hook: Mutex<Option<DeliveryHook>>,
}

impl<T, E> OutcomeSlot<T, E> {
    /// Creates an empty outcome slot associated with the operation core.
    ///
    /// # Parameters
    ///
    /// - `core`: Shared lifecycle state updated when an outcome becomes
    ///   available or is consumed.
    ///
    /// # Returns
    ///
    /// An empty slot ready to publish an outcome.
    pub(crate) fn new(core: Arc<OperationCore>) -> Self {
        Self {
            core,
            cell: StateCell::new(),
            delivery: AtomicU8::new(DELIVERY_PENDING),
            delivery_hook: Mutex::new(None),
        }
    }

    /// Installs the internal, identity-capturing permit handoff callback.
    ///
    /// If delivery or discard already happened, invokes the callback
    /// immediately with that result.
    ///
    /// # Parameters
    ///
    /// - `self`: The outcome slot receiving the callback.
    /// - `hook`: One-shot callback invoked when the outcome is delivered or
    ///   discarded.
    pub(crate) fn set_delivery_hook(&self, hook: impl FnOnce(OutcomeDelivery) + Send + 'static) {
        let (hook, delivery) = {
            let mut slot = lock(&self.delivery_hook);
            assert!(slot.is_none(), "outcome delivery hook is one-shot");
            *slot = Some(Box::new(hook));
            match self.delivery.load(Ordering::Acquire) {
                DELIVERY_TAKEN => (slot.take(), Some(OutcomeDelivery::Delivered)),
                DELIVERY_DISCARDED => (slot.take(), Some(OutcomeDelivery::Discarded)),
                _ => (None, None),
            }
        };
        if let (Some(hook), Some(delivery)) = (hook, delivery) {
            hook(delivery);
        }
    }

    /// Publishes an outcome and marks the operation as having an available
    /// result.
    ///
    /// # Parameters
    ///
    /// - `self`: The slot that receives the outcome.
    /// - `outcome`: Successful or failed result to publish.
    ///
    /// # Returns
    ///
    /// `Ok(())` if the outcome was published, or the unchanged outcome if the
    ///   slot was already completed.
    pub(crate) fn publish(&self, outcome: OperationOutcome<T, E>) -> Result<(), OperationOutcome<T, E>> {
        match self.cell.publish(outcome) {
            Ok(()) => {
                self.core.mark_available();
                Ok(())
            }
            Err(outcome) => Err(outcome),
        }
    }

    /// Requests that the operation stop for the given reason.
    ///
    /// # Parameters
    ///
    /// - `self`: The slot whose operation receives the stop request.
    /// - `reason`: Cause recorded if this request is accepted.
    ///
    /// # Returns
    ///
    /// `true` if the request was accepted; `false` if completion already won.
    pub(crate) fn request_stop(&self, reason: StopReason) -> bool {
        let accepted_at = self.core.clock.as_ref().map(|clock| clock.now());
        if !self.cell.request_stop(reason, accepted_at) {
            return false;
        }
        if let Some(report) = self.cell.stop_report() {
            self.core.record_stop(report);
        }
        self.core.mark_available();
        true
    }

    /// Takes an outcome without blocking when one is ready.
    ///
    /// # Parameters
    ///
    /// - `self`: The slot queried for a ready outcome.
    ///
    /// # Returns
    ///
    /// The outcome when ready, or `None` while it is still pending.
    ///
    /// # Errors
    ///
    /// Returns the stored outcome error if the result was discarded.
    pub(in crate::v02::operation) fn take_if_ready(&self) -> Result<Option<OperationOutcome<T, E>>, OutcomeError> {
        let result = self.cell.take_if_ready()?;
        if result.is_some() {
            self.core.mark_taken();
            self.notify_delivery(OutcomeDelivery::Delivered);
        }
        Ok(result)
    }

    /// Blocks until the operation outcome can be taken.
    ///
    /// # Parameters
    ///
    /// - `self`: The slot whose outcome is awaited.
    ///
    /// # Returns
    ///
    /// The operation outcome.
    ///
    /// # Errors
    ///
    /// Returns [`OutcomeError::WouldDeadlock`] on a blocking worker, or the
    /// stored outcome error if the result was discarded.
    pub(in crate::v02::operation) fn take_blocking(&self) -> Result<OperationOutcome<T, E>, OutcomeError> {
        if self.core.is_blocking_worker() {
            return Err(OutcomeError::WouldDeadlock);
        }
        let outcome = self.cell.take_result_blocking()?;
        self.core.mark_taken();
        self.notify_delivery(OutcomeDelivery::Delivered);
        Ok(outcome)
    }

    /// Asynchronously waits for and takes the operation outcome.
    ///
    /// # Parameters
    ///
    /// - `self`: The slot whose outcome is awaited.
    ///
    /// # Returns
    ///
    /// The operation outcome.
    ///
    /// # Errors
    ///
    /// Returns the stored outcome error if the result was discarded.
    pub(in crate::v02::operation) async fn take_async(&self) -> Result<OperationOutcome<T, E>, OutcomeError> {
        let outcome = self.cell.result().await?;
        self.core.mark_taken();
        self.notify_delivery(OutcomeDelivery::Delivered);
        Ok(outcome)
    }

    /// Discards an available outcome and notifies the delivery hook.
    ///
    /// Does nothing while the outcome remains pending or after it was already
    /// taken or discarded.
    ///
    /// # Parameters
    ///
    /// - `self`: The slot whose stored outcome may be discarded.
    pub(in crate::v02::operation) fn discard(&self) {
        if self.cell.discard_result() {
            self.core.mark_discarded();
            self.notify_delivery(OutcomeDelivery::Discarded);
        }
    }

    /// Returns the stop report recorded for this operation, if any.
    ///
    /// # Parameters
    ///
    /// - `self`: The slot whose stop report is read.
    ///
    /// # Returns
    ///
    /// The accepted stop request, if one was recorded.
    pub(crate) fn stop_report(&self) -> Option<StopReport> {
        self.cell.stop_report()
    }

    /// Records outcome delivery and invokes the installed handoff callback.
    ///
    /// # Parameters
    ///
    /// - `self`: The slot whose outcome was consumed or discarded.
    /// - `delivery`: How ownership of the outcome was resolved.
    fn notify_delivery(&self, delivery: OutcomeDelivery) {
        let next = match delivery {
            OutcomeDelivery::Delivered => DELIVERY_TAKEN,
            OutcomeDelivery::Discarded => DELIVERY_DISCARDED,
        };
        if self
            .delivery
            .compare_exchange(DELIVERY_PENDING, next, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let hook = lock(&self.delivery_hook).take();
        if let Some(hook) = hook {
            hook(delivery);
        }
    }
}

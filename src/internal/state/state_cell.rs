// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The shared result cell and its publication/consumption operations.

use std::sync::Arc;
use std::sync::Mutex;

use super::ResultFuture;
use super::ResultSlot;
use super::State;
use super::lock;
use crate::internal::notify::Notify;
use crate::report::MonotonicTime;
use crate::report::ObservationError;
use crate::report::OperationOutcome;
use crate::report::SettlementReport;
use crate::report::StopReason;
use crate::report::StopReport;

/// Shares a single-consumer business result and repeatable settlement snapshot.
///
/// # Type Parameters
///
/// - `T`: The value type carried by a successful business outcome.
/// - `E`: The error type carried by a failed business outcome.
pub(crate) struct StateCell<T, E> {
    /// Serializes all business, stop, and settlement facts.
    state: Mutex<State<T, E>>,
    /// Notifies both blocking and async result observers after publication.
    pub(super) changed: Notify,
}

impl<T, E> StateCell<T, E> {
    /// Creates a pending result cell with no stop or settlement facts.
    ///
    /// # Returns
    ///
    /// A cell with an empty result slot, no stop report, and no settlement.
    #[must_use]
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(State {
                result: ResultSlot::Pending,
                stop: None,
                success_published: false,
                settlement: None,
            }),
            changed: Notify::new(),
        }
    }

    /// Takes a ready result or reports the terminal state that prevents access.
    ///
    /// # Returns
    ///
    /// Returns `Some` with the consumed result, or `None` while it is pending.
    ///
    /// # Errors
    ///
    /// Returns `AlreadyTaken` or `ResultDiscarded` for those terminal states.
    pub(super) fn take_if_ready(&self) -> Result<Option<OperationOutcome<T, E>>, ObservationError> {
        let mut state = lock(&self.state);
        match std::mem::replace(&mut state.result, ResultSlot::Taken) {
            ResultSlot::Pending => {
                state.result = ResultSlot::Pending;
                Ok(None)
            }
            ResultSlot::Ready(result) => Ok(Some(result)),
            ResultSlot::Taken => {
                state.result = ResultSlot::Taken;
                Err(ObservationError::AlreadyTaken)
            }
            ResultSlot::Discarded => {
                state.result = ResultSlot::Discarded;
                Err(ObservationError::ResultDiscarded)
            }
        }
    }

    /// Publishes the first business outcome. A previously accepted stop wins.
    ///
    /// # Parameters
    ///
    /// - `outcome`: The result to publish if the result slot remains pending.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` when this outcome is published.
    ///
    /// # Errors
    ///
    /// Returns the unconsumed `outcome` when another outcome or stop has
    /// already claimed the result slot.
    pub(crate) fn publish(&self, outcome: OperationOutcome<T, E>) -> Result<(), OperationOutcome<T, E>> {
        let mut state = lock(&self.state);
        if !matches!(state.result, ResultSlot::Pending) || state.stop.is_some() {
            drop(state);
            return Err(outcome);
        }
        if matches!(&outcome, OperationOutcome::Succeeded(_)) {
            state.success_published = true;
        }
        if let OperationOutcome::Stopped(report) = &outcome {
            state.stop = Some(report.clone());
        }
        state.result = ResultSlot::Ready(outcome);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    /// Accepts the first stop reason if no successful outcome has been
    /// published.
    ///
    /// # Parameters
    ///
    /// - `reason`: The stop reason to record.
    /// - `accepted_at`: The monotonic time at which the stop was accepted, if
    ///   known.
    ///
    /// # Returns
    ///
    /// Returns `true` if this call records the first accepted stop, or `false`
    /// if success or an earlier stop already determines the result.
    #[must_use]
    pub(crate) fn request_stop(&self, reason: StopReason, accepted_at: Option<MonotonicTime>) -> bool {
        let accepted = {
            let mut state = lock(&self.state);
            if state.success_published || state.stop.is_some() {
                false
            } else {
                let report = StopReport { reason, accepted_at };
                state.stop = Some(report.clone());
                if matches!(state.result, ResultSlot::Pending) {
                    state.result = ResultSlot::Ready(OperationOutcome::Stopped(report));
                }
                true
            }
        };
        if accepted {
            self.changed.notify_waiters();
        }
        accepted
    }

    /// Takes the typed result once; later consumers receive `AlreadyTaken`.
    ///
    /// # Returns
    ///
    /// The typed outcome after it becomes available.
    ///
    /// # Errors
    ///
    /// Returns `AlreadyTaken` or `ResultDiscarded` when the result slot is no
    /// longer available to this consumer.
    pub(crate) fn take_result_blocking(&self) -> Result<OperationOutcome<T, E>, ObservationError> {
        loop {
            let generation = self.changed.generation();
            if let Some(result) = self.take_if_ready()? {
                return Ok(result);
            }
            self.changed.wait_blocking_since(generation);
        }
    }

    /// Observes and consumes the typed result asynchronously.
    ///
    /// # Returns
    ///
    /// A future that consumes the result when it becomes ready.
    #[inline]
    pub(crate) fn result(&self) -> ResultFuture<'_, T, E> {
        ResultFuture {
            cell: self,
            notification: None,
        }
    }

    /// Discards an available result without dropping its user value under the
    /// state lock.
    ///
    /// # Returns
    ///
    /// Returns `true` if a ready result was discarded, or `false` if the result
    /// was pending, already taken, or already discarded.
    #[must_use]
    pub(crate) fn discard_result(&self) -> bool {
        let discarded = {
            let mut state = lock(&self.state);
            match std::mem::replace(&mut state.result, ResultSlot::Discarded) {
                ResultSlot::Ready(result) => Some(result),
                ResultSlot::Pending => {
                    state.result = ResultSlot::Pending;
                    None
                }
                ResultSlot::Taken => {
                    state.result = ResultSlot::Taken;
                    None
                }
                ResultSlot::Discarded => None,
            }
        };
        if let Some(result) = discarded {
            drop(result);
            self.changed.notify_waiters();
            true
        } else {
            false
        }
    }

    /// Publishes one immutable terminal snapshot for repeatable observation.
    ///
    /// # Parameters
    ///
    /// - `report`: The terminal settlement snapshot to publish.
    ///
    /// # Returns
    ///
    /// The shared snapshot retained for later observers.
    ///
    /// # Errors
    ///
    /// Returns `report` unchanged if another settlement snapshot was already
    /// published.
    pub(crate) fn publish_settlement(
        &self,
        report: SettlementReport,
    ) -> Result<Arc<SettlementReport>, SettlementReport> {
        let mut state = lock(&self.state);
        if state.settlement.is_some() {
            drop(state);
            return Err(report);
        }
        let report = Arc::new(report);
        state.settlement = Some(Arc::clone(&report));
        drop(state);
        self.changed.notify_waiters();
        Ok(report)
    }

    /// Returns the stable settlement snapshot for repeated observation.
    ///
    /// # Returns
    ///
    /// Returns `Some` with the shared snapshot after publication, or `None`
    /// before a settlement exists.
    #[inline]
    pub(crate) fn settlement(&self) -> Option<Arc<SettlementReport>> {
        lock(&self.state).settlement.as_ref().map(Arc::clone)
    }

    /// Returns the first accepted stop fact without consuming the business
    /// result.
    ///
    /// # Returns
    ///
    /// Returns `Some` with the first accepted stop report, or `None` if no stop
    /// has been accepted.
    #[inline]
    pub(crate) fn stop_report(&self) -> Option<StopReport> {
        lock(&self.state).stop.clone()
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

use super::ResultFuture;
use super::ResultSlot;
use super::State;
use super::lock;
use crate::v02::internal::notify::Notify;
use crate::v02::protocol::MonotonicTime;
use crate::v02::report::OperationOutcome;
use crate::v02::report::OutcomeError;
use crate::v02::report::StopReason;
use crate::v02::report::StopReport;

/// Single-consumer business-result state plus independently published lifecycle
/// facts.
///
/// Success and stop facts compete for the first result slot, while stop state
/// remains observable independently after publication.
///
/// # Type Parameters
///
/// - `T`: success value carried by an operation outcome.
/// - `E`: failure value carried by an operation outcome.
#[must_use]
pub(crate) struct StateCell<T, E> {
    /// Mutex-protected result slot, first stop report, and success marker.
    pub(in crate::v02::internal::state) state: Mutex<State<T, E>>,
    /// Wakes blocking and asynchronous readers after a lifecycle fact changes.
    pub(in crate::v02::internal::state) changed: Notify,
}

impl<T, E> StateCell<T, E> {
    /// Creates a cell with no outcome or accepted stop request.
    ///
    /// # Returns
    ///
    /// A single-consumer cell ready to publish its first outcome or stop fact.
    pub(crate) fn new() -> Self {
        Self {
            state: Mutex::new(State {
                result: ResultSlot::Pending,
                stop: None,
                success_published: false,
            }),
            changed: Notify::new(),
        }
    }

    /// Creates an asynchronous one-shot receiver borrowing this cell.
    ///
    /// The returned future registers its waker while the result is pending and
    /// consumes the outcome when it becomes ready.
    ///
    /// # Parameters
    ///
    /// - `self`: The cell whose outcome is awaited.
    ///
    /// # Returns
    ///
    /// A future that resolves to the stored outcome or a terminal access error.
    pub(crate) fn result(&self) -> ResultFuture<'_, T, E> {
        ResultFuture {
            cell: self,
            notification: None,
        }
    }

    /// Returns the first accepted stop report, if any.
    ///
    /// # Parameters
    ///
    /// - `self`: The cell whose stop report is read.
    ///
    /// # Returns
    ///
    /// `Some(report)` after a stop was accepted, or `None` while no stop has
    /// been accepted.
    pub(crate) fn stop_report(&self) -> Option<StopReport> {
        lock(&self.state).stop.clone()
    }

    /// Publishes an outcome if the result slot is still pending and no stop
    /// won.
    ///
    /// A successful outcome records that success won stop arbitration. A
    /// stopped outcome also records its stop report. On publication, all
    /// current readers are notified; a rejected outcome is returned
    /// unchanged to the caller.
    ///
    /// # Parameters
    ///
    /// - `self`: The cell receiving the outcome.
    /// - `outcome`: success, failure, or stop fact to publish.
    ///
    /// # Returns
    ///
    /// `Ok(())` when stored, or `Err(outcome)` when the slot is no longer
    /// pending or a stop request has already been accepted.
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

    /// Accepts the first stop unless success or an earlier stop already won.
    ///
    /// An accepted stop fills a pending result slot with a stopped outcome and
    /// wakes readers. It does not replace an outcome that is already ready.
    ///
    /// # Parameters
    ///
    /// - `self`: The cell receiving the stop request.
    /// - `reason`: reason recorded for the accepted stop.
    /// - `accepted_at`: monotonic acceptance time, or `None` when unavailable.
    ///
    /// # Returns
    ///
    /// `true` if this call accepted the stop; `false` if success or an earlier
    /// stop had already won.
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

    /// Takes a ready result or reports why the result is unavailable.
    ///
    /// A pending slot is left unchanged. A ready result is consumed exactly
    /// once; taken and discarded slots remain terminal.
    ///
    /// # Parameters
    ///
    /// - `self`: The cell whose ready result is taken.
    ///
    /// # Returns
    ///
    /// `Ok(None)` while pending, `Ok(Some(outcome))` when this call takes the
    /// outcome, or an error for a terminal taken/discarded slot.
    ///
    /// # Errors
    ///
    /// Returns [`OutcomeError::AlreadyTaken`] after another consumer took the
    /// result, or [`OutcomeError::Discarded`] after delivery was discarded.
    pub(crate) fn take_if_ready(&self) -> Result<Option<OperationOutcome<T, E>>, OutcomeError> {
        let mut state = lock(&self.state);
        match std::mem::replace(&mut state.result, ResultSlot::Taken) {
            ResultSlot::Pending => {
                state.result = ResultSlot::Pending;
                Ok(None)
            }
            ResultSlot::Ready(result) => Ok(Some(result)),
            ResultSlot::Taken => {
                state.result = ResultSlot::Taken;
                Err(OutcomeError::AlreadyTaken)
            }
            ResultSlot::Discarded => {
                state.result = ResultSlot::Discarded;
                Err(OutcomeError::Discarded)
            }
        }
    }

    /// Takes the outcome once, blocking the current thread until it is
    /// published.
    ///
    /// # Parameters
    ///
    /// - `self`: The cell whose outcome is awaited.
    ///
    /// # Returns
    ///
    /// The stored operation outcome after it becomes ready.
    ///
    /// # Errors
    ///
    /// Returns [`OutcomeError::AlreadyTaken`] if another consumer took the
    /// outcome, or [`OutcomeError::Discarded`] if delivery was discarded.
    pub(crate) fn take_result_blocking(&self) -> Result<OperationOutcome<T, E>, OutcomeError> {
        loop {
            let generation = self.changed.generation();
            if let Some(result) = self.take_if_ready()? {
                return Ok(result);
            }
            self.changed.wait_blocking_since(generation);
        }
    }

    /// Discards delivery rights, dropping a ready outcome outside the state
    /// lock.
    ///
    /// Waiters are notified if the slot changes. A taken result cannot be
    /// discarded, and repeated discard requests leave the state unchanged.
    ///
    /// # Parameters
    ///
    /// - `self`: The cell whose delivery rights are discarded.
    ///
    /// # Returns
    ///
    /// `true` when this call changes a pending or ready slot to discarded;
    /// `false` when the result was already taken or discarded.
    pub(crate) fn discard_result(&self) -> bool {
        let (discarded, changed) = {
            let mut state = lock(&self.state);
            match std::mem::replace(&mut state.result, ResultSlot::Discarded) {
                ResultSlot::Ready(result) => (Some(result), true),
                ResultSlot::Pending => (None, true),
                ResultSlot::Taken => {
                    state.result = ResultSlot::Taken;
                    (None, false)
                }
                ResultSlot::Discarded => (None, false),
            }
        };
        drop(discarded);
        if changed {
            self.changed.notify_waiters();
        }
        changed
    }
}

//! Single-consumer result publication and repeatable settlement state.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};

use crate::v02::internal::notify::Notify;
use crate::v02::protocol::MonotonicTime;
use crate::v02::report::{
    OperationOutcome, OutcomeError, SettlementReport, StopReason, StopReport,
};

/// Single-consumer business-result state plus independently published lifecycle facts.
pub(crate) struct StateCell<T, E> {
    state: Mutex<State<T, E>>,
    changed: Notify,
}

struct State<T, E> {
    result: ResultSlot<T, E>,
    stop: Option<StopReport>,
    success_published: bool,
    settlement: Option<Arc<SettlementReport>>,
}

enum ResultSlot<T, E> {
    Pending,
    Ready(OperationOutcome<T, E>),
    Taken,
    Discarded,
}

impl<T, E> StateCell<T, E> {
    /// Creates a pending cell.
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

    /// Takes a ready result or reports the terminal state preventing access.
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

    /// Publishes the first outcome, returning it unchanged if another fact won.
    pub(crate) fn publish(
        &self,
        outcome: OperationOutcome<T, E>,
    ) -> Result<(), OperationOutcome<T, E>> {
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
    pub(crate) fn request_stop(
        &self,
        reason: StopReason,
        accepted_at: Option<MonotonicTime>,
    ) -> bool {
        let accepted = {
            let mut state = lock(&self.state);
            if state.success_published || state.stop.is_some() {
                false
            } else {
                let report = StopReport {
                    reason,
                    accepted_at,
                };
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

    /// Takes the outcome once, blocking until it is published.
    pub(crate) fn take_result_blocking(&self) -> Result<OperationOutcome<T, E>, OutcomeError> {
        loop {
            let generation = self.changed.generation();
            if let Some(result) = self.take_if_ready()? {
                return Ok(result);
            }
            self.changed.wait_blocking_since(generation);
        }
    }

    /// Creates an async one-shot result receiver.
    pub(crate) fn result(&self) -> ResultFuture<'_, T, E> {
        ResultFuture {
            cell: self,
            notification: None,
        }
    }

    /// Discards delivery rights, dropping a ready outcome outside the state lock.
    ///
    /// Returns true when this call changes Pending or Ready to Discarded.
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

    /// Publishes one immutable settlement snapshot.
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

    /// Returns a repeatable stable settlement snapshot.
    pub(crate) fn settlement(&self) -> Option<Arc<SettlementReport>> {
        lock(&self.state).settlement.as_ref().map(Arc::clone)
    }

    /// Returns the first accepted stop report.
    pub(crate) fn stop_report(&self) -> Option<StopReport> {
        lock(&self.state).stop.clone()
    }
}

/// Async receiver for the single typed outcome.
pub(crate) struct ResultFuture<'a, T, E> {
    cell: &'a StateCell<T, E>,
    notification: Option<crate::v02::internal::notify::Notified>,
}

impl<T, E> Future for ResultFuture<'_, T, E> {
    type Output = Result<OperationOutcome<T, E>, OutcomeError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let generation = self.cell.changed.generation();
            match self.cell.take_if_ready() {
                Ok(Some(result)) => return Poll::Ready(Ok(result)),
                Err(error) => return Poll::Ready(Err(error)),
                Ok(None) => {}
            }
            if self.notification.is_none() {
                self.notification = Some(self.cell.changed.notified_since(generation));
            }
            match Pin::new(self.notification.as_mut().expect("notification was set")).poll(context)
            {
                Poll::Ready(()) => self.notification = None,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v02::report::OperationOutcome;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::sync::Weak;

    struct LockCheckingDrop {
        cell: Weak<StateCell<LockCheckingDrop, ()>>,
        dropped_outside_lock: Arc<AtomicBool>,
    }

    impl Drop for LockCheckingDrop {
        fn drop(&mut self) {
            if let Some(cell) = self.cell.upgrade() {
                self.dropped_outside_lock
                    .store(cell.state.try_lock().is_ok(), Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn discard_drops_ready_value_outside_state_lock() {
        let cell = Arc::new(StateCell::<LockCheckingDrop, ()>::new());
        let dropped_outside_lock = Arc::new(AtomicBool::new(false));
        let value = LockCheckingDrop {
            cell: Arc::downgrade(&cell),
            dropped_outside_lock: Arc::clone(&dropped_outside_lock),
        };
        assert!(cell.publish(OperationOutcome::Succeeded(value)).is_ok());
        assert!(cell.discard_result());
        assert!(dropped_outside_lock.load(Ordering::SeqCst));
    }

    #[test]
    fn cancelled_result_wait_keeps_delivery() {
        let cell = StateCell::<Box<u32>, ()>::new();
        let mut future = Box::pin(cell.result());
        let waker = std::task::Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(future.as_mut().poll(&mut context).is_pending());
        drop(future);

        assert!(cell
            .publish(OperationOutcome::Succeeded(Box::new(91)))
            .is_ok());
        let outcome = cell
            .take_if_ready()
            .expect("cancelled observation must not take result")
            .expect("published result must remain available");
        assert!(matches!(outcome, OperationOutcome::Succeeded(value) if *value == 91));
    }

    #[test]
    fn discard_pending_prevents_later_delivery() {
        let cell = StateCell::<Box<u32>, ()>::new();
        assert!(cell.discard_result());
        let outcome = OperationOutcome::Succeeded(Box::new(17));
        let returned = cell
            .publish(outcome)
            .expect_err("discarded delivery must reject publication");
        assert!(matches!(returned, OperationOutcome::Succeeded(value) if *value == 17));
        assert_eq!(cell.take_if_ready().unwrap_err(), OutcomeError::Discarded);
    }

    #[test]
    fn take_once_preserves_owned_value() {
        let cell = StateCell::<Box<u32>, ()>::new();
        let owned = Box::new(73);
        assert!(cell.publish(OperationOutcome::Succeeded(owned)).is_ok());
        let received = cell
            .take_if_ready()
            .expect("result should be available")
            .expect("result should be ready");
        match received {
            OperationOutcome::Succeeded(value) => assert_eq!(*value, 73),
            _ => panic!("expected successful outcome"),
        }
        assert_eq!(
            cell.take_if_ready().unwrap_err(),
            OutcomeError::AlreadyTaken
        );
    }
}

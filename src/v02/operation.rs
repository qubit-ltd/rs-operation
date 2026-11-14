//! Typed, single-consumer outcomes and untyped lifecycle observation.

use crate::v02::internal::notify::{Notified, Notify};
use crate::v02::internal::state::StateCell;
use crate::v02::protocol::{ClockSource, Deadline, TimerRegistration};
use crate::v02::report::{
    ExecutionPhase, ObservationError, OperationId, OperationOutcome, OperationSnapshot,
    OutcomeAvailability, OutcomeError, ResourceProgress, SettlementReport, SettlementWait,
    StopReason, StopReport, StopRequestStatus,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

const AVAILABILITY_PENDING: u8 = 0;
const AVAILABILITY_AVAILABLE: u8 = 1;
const AVAILABILITY_TAKEN: u8 = 2;
const AVAILABILITY_DISCARDED: u8 = 3;

/// Untyped lifecycle facts shared with read-only observers.
///
/// The core deliberately stores no typed outcome. [`OutcomeSlot`] owns the
/// only typed state, while snapshots expose only availability and reports.
pub(crate) struct OperationCore {
    id: OperationId,
    facts: Mutex<CoreFacts>,
    availability: AtomicU8,
    changed: Notify,
    changed_blocking: Condvar,
    clock: Option<Arc<dyn ClockSource>>,
    blocking_worker: Arc<dyn Fn() -> bool + Send + Sync>,
}

struct CoreFacts {
    execution: ExecutionPhase,
    stop: Option<StopReport>,
    resources: Option<ResourceProgress>,
    settlement: Option<Arc<SettlementReport>>,
}

impl OperationCore {
    /// Creates an untyped operation core with its initial queued snapshot.
    pub(crate) fn new(id: OperationId) -> Arc<Self> {
        Self::new_with_observers(id, None, Arc::new(|| false))
    }

    pub(crate) fn new_with_observers(
        id: OperationId,
        clock: Option<Arc<dyn ClockSource>>,
        blocking_worker: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id,
            facts: Mutex::new(CoreFacts {
                execution: ExecutionPhase::Queued,
                stop: None,
                resources: None,
                settlement: None,
            }),
            availability: AtomicU8::new(AVAILABILITY_PENDING),
            changed: Notify::new(),
            changed_blocking: Condvar::new(),
            clock,
            blocking_worker,
        })
    }

    pub(crate) fn id(&self) -> OperationId {
        self.id
    }

    pub(crate) fn snapshot(&self) -> OperationSnapshot {
        let facts = lock(&self.facts);
        OperationSnapshot {
            operation_id: self.id,
            execution: facts.execution,
            outcome: self.outcome_availability(),
            stop: facts.stop.clone(),
            resources: facts.resources,
            settlement: facts.settlement.as_ref().map(Arc::clone),
        }
    }

    fn outcome_availability(&self) -> OutcomeAvailability {
        match self.availability.load(Ordering::Acquire) {
            AVAILABILITY_AVAILABLE => OutcomeAvailability::Available,
            AVAILABILITY_TAKEN => OutcomeAvailability::Taken,
            AVAILABILITY_DISCARDED => OutcomeAvailability::Discarded,
            _ => OutcomeAvailability::Pending,
        }
    }

    pub(crate) fn execution(&self) -> ExecutionPhase {
        lock(&self.facts).execution
    }

    pub(crate) fn set_execution(&self, execution: ExecutionPhase) {
        lock(&self.facts).execution = execution;
        self.notify_change();
    }

    pub(crate) fn set_resource_progress(&self, resources: Option<ResourceProgress>) {
        lock(&self.facts).resources = resources;
        self.notify_change();
    }

    fn mark_available(&self) {
        let _ = self.availability.compare_exchange(
            AVAILABILITY_PENDING,
            AVAILABILITY_AVAILABLE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.notify_change();
    }

    fn mark_taken(&self) {
        self.availability
            .store(AVAILABILITY_TAKEN, Ordering::Release);
        self.notify_change();
    }

    fn mark_discarded(&self) {
        self.availability
            .store(AVAILABILITY_DISCARDED, Ordering::Release);
        self.notify_change();
    }

    fn record_stop(&self, stop: StopReport) {
        let mut facts = lock(&self.facts);
        if facts.stop.is_none() {
            facts.stop = Some(stop);
        }
        drop(facts);
        self.notify_change();
    }

    /// Publishes exactly one immutable settlement snapshot.
    pub(crate) fn publish_settlement(
        &self,
        report: SettlementReport,
    ) -> Result<Arc<SettlementReport>, SettlementReport> {
        let mut facts = lock(&self.facts);
        if facts.settlement.is_some() {
            drop(facts);
            return Err(report);
        }
        let report = Arc::new(report);
        facts.settlement = Some(Arc::clone(&report));
        drop(facts);
        self.notify_change();
        Ok(report)
    }

    fn settlement(&self) -> Option<Arc<SettlementReport>> {
        lock(&self.facts).settlement.as_ref().map(Arc::clone)
    }

    fn notify_change(&self) {
        self.changed.notify_waiters();
        self.changed_blocking.notify_all();
    }

    fn wait_settlement_blocking(&self) -> Arc<SettlementReport> {
        let mut facts = lock(&self.facts);
        loop {
            if let Some(report) = facts.settlement.as_ref() {
                return Arc::clone(report);
            }
            facts = self
                .changed_blocking
                .wait(facts)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn wait_settlement_for(&self, timeout: Duration) -> SettlementWait {
        let until = Instant::now().checked_add(timeout);
        let mut facts = lock(&self.facts);
        loop {
            if let Some(report) = facts.settlement.as_ref() {
                return SettlementWait::Ready(Arc::clone(report));
            }
            let Some(until) = until else {
                facts = self
                    .changed_blocking
                    .wait(facts)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                continue;
            };
            let now = Instant::now();
            if now >= until {
                drop(facts);
                return SettlementWait::Pending(self.snapshot());
            }
            let duration = until.saturating_duration_since(now);
            let (next, result) = self
                .changed_blocking
                .wait_timeout(facts, duration)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            facts = next;
            if result.timed_out() && facts.settlement.is_none() {
                drop(facts);
                return SettlementWait::Pending(self.snapshot());
            }
        }
    }

    fn is_blocking_worker(&self) -> bool {
        (self.blocking_worker)()
    }

    fn wait_settlement_async(&self) -> SettlementFuture<'_> {
        SettlementFuture {
            core: self,
            notification: None,
        }
    }

    fn wait_settlement_until(&self, deadline: Deadline) -> SettlementUntil<'_> {
        SettlementUntil {
            core: self,
            deadline,
            notification: None,
            timer: None,
        }
    }
}

/// The one typed outcome cell for an operation.
///
/// This is an adapter over T02's [`StateCell`], not a second result state
/// machine. It mirrors only the untyped availability bit into [`OperationCore`]
/// so observers never retain `T` or `E`.
pub(crate) struct OutcomeSlot<T, E> {
    core: Arc<OperationCore>,
    cell: StateCell<T, E>,
}

impl<T, E> OutcomeSlot<T, E> {
    pub(crate) fn new(core: Arc<OperationCore>) -> Self {
        Self {
            core,
            cell: StateCell::new(),
        }
    }

    pub(crate) fn publish(
        &self,
        outcome: OperationOutcome<T, E>,
    ) -> Result<(), OperationOutcome<T, E>> {
        match self.cell.publish(outcome) {
            Ok(()) => {
                self.core.mark_available();
                Ok(())
            }
            Err(outcome) => Err(outcome),
        }
    }

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

    fn take_if_ready(&self) -> Result<Option<OperationOutcome<T, E>>, OutcomeError> {
        let result = self.cell.take_if_ready()?;
        if result.is_some() {
            self.core.mark_taken();
        }
        Ok(result)
    }

    fn take_blocking(&self) -> Result<OperationOutcome<T, E>, OutcomeError> {
        if self.core.is_blocking_worker() {
            return Err(OutcomeError::WouldDeadlock);
        }
        let outcome = self.cell.take_result_blocking()?;
        self.core.mark_taken();
        Ok(outcome)
    }

    async fn take_async(&self) -> Result<OperationOutcome<T, E>, OutcomeError> {
        let outcome = self.cell.result().await?;
        self.core.mark_taken();
        Ok(outcome)
    }

    fn discard(&self) {
        if self.cell.discard_result() {
            self.core.mark_discarded();
        }
    }

    pub(crate) fn stop_report(&self) -> Option<StopReport> {
        self.cell.stop_report()
    }
}

/// The synchronous one-shot receiver for an operation outcome.
pub struct OperationHandle<T: Send + 'static, E: Send + 'static> {
    parts: Option<HandleParts<T, E>>,
}

/// The asynchronous one-shot receiver for an operation outcome.
pub struct AsyncOperationHandle<T: Send + 'static, E: Send + 'static> {
    parts: Option<HandleParts<T, E>>,
}

struct HandleParts<T, E> {
    core: Arc<OperationCore>,
    slot: Arc<OutcomeSlot<T, E>>,
    stop: Arc<dyn StopControl>,
}

/// Untyped stop-forwarding port supplied by the operation owner.
pub(crate) trait StopControl: Send + Sync {
    fn request_stop(&self, reason: StopReason);
}

impl<T: Send + 'static, E: Send + 'static> OperationHandle<T, E> {
    pub(crate) fn new(
        core: Arc<OperationCore>,
        slot: Arc<OutcomeSlot<T, E>>,
        stop: Arc<dyn StopControl>,
    ) -> Self {
        Self {
            parts: Some(HandleParts { core, slot, stop }),
        }
    }

    /// Returns this operation's stable ID.
    pub fn id(&self) -> OperationId {
        self.parts().core.id()
    }

    /// Requests the first stop reason accepted by this operation.
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
    pub fn observer(&self) -> OperationObserver {
        OperationObserver {
            core: Arc::clone(&self.parts().core),
        }
    }

    /// Returns the latest untyped lifecycle snapshot.
    pub fn snapshot(&self) -> OperationSnapshot {
        self.parts().core.snapshot()
    }

    /// Blocks until the unique typed outcome is available and takes it.
    pub fn wait_outcome(&mut self) -> Result<OperationOutcome<T, E>, OutcomeError> {
        self.parts().slot.take_blocking()
    }

    /// Takes the typed outcome if it is already available.
    pub fn try_take_outcome(&mut self) -> Result<Option<OperationOutcome<T, E>>, OutcomeError> {
        self.parts().slot.take_if_ready()
    }

    /// Blocks until the stable settlement report is published.
    pub fn wait_settlement(&self) -> Result<Arc<SettlementReport>, ObservationError> {
        let core = &self.parts().core;
        if core.is_blocking_worker() {
            return Err(ObservationError::WouldDeadlock);
        }
        Ok(core.wait_settlement_blocking())
    }

    /// Waits for settlement for at most `timeout`, returning the current snapshot on timeout.
    pub fn wait_settlement_for(
        &self,
        timeout: Duration,
    ) -> Result<SettlementWait, ObservationError> {
        let core = &self.parts().core;
        if core.is_blocking_worker() {
            return Err(ObservationError::WouldDeadlock);
        }
        Ok(core.wait_settlement_for(timeout))
    }

    /// Moves the unique outcome receiver into its async view without stopping the operation.
    pub fn into_async(mut self) -> AsyncOperationHandle<T, E> {
        AsyncOperationHandle {
            parts: self.parts.take(),
        }
    }

    /// Discards the typed outcome and returns a read-only observer without requesting stop.
    pub fn into_observer(mut self) -> OperationObserver {
        let parts = self.parts.take().expect("handle parts are present");
        parts.slot.discard();
        OperationObserver { core: parts.core }
    }

    fn parts(&self) -> &HandleParts<T, E> {
        self.parts.as_ref().expect("handle parts are present")
    }
}

impl<T: Send + 'static, E: Send + 'static> AsyncOperationHandle<T, E> {
    pub(crate) fn new(
        core: Arc<OperationCore>,
        slot: Arc<OutcomeSlot<T, E>>,
        stop: Arc<dyn StopControl>,
    ) -> Self {
        Self {
            parts: Some(HandleParts { core, slot, stop }),
        }
    }

    /// Returns this operation's stable ID.
    pub fn id(&self) -> OperationId {
        self.parts().core.id()
    }

    /// Requests the first stop reason accepted by this operation.
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
    pub fn observer(&self) -> AsyncOperationObserver {
        AsyncOperationObserver {
            core: Arc::clone(&self.parts().core),
        }
    }

    /// Returns the latest untyped lifecycle snapshot.
    pub fn snapshot(&self) -> OperationSnapshot {
        self.parts().core.snapshot()
    }

    /// Asynchronously waits for and takes the unique typed outcome.
    pub async fn wait_outcome(&mut self) -> Result<OperationOutcome<T, E>, OutcomeError> {
        self.parts().slot.take_async().await
    }

    /// Takes the typed outcome if it is already available.
    pub fn try_take_outcome(&mut self) -> Result<Option<OperationOutcome<T, E>>, OutcomeError> {
        self.parts().slot.take_if_ready()
    }

    /// Asynchronously waits for the immutable settlement report.
    pub async fn wait_settlement(&self) -> Arc<SettlementReport> {
        self.parts().core.wait_settlement_async().await
    }

    /// Moves the unique outcome receiver into its sync view without stopping the operation.
    pub fn into_sync(mut self) -> OperationHandle<T, E> {
        OperationHandle {
            parts: self.parts.take(),
        }
    }

    /// Discards the typed outcome and returns a read-only observer without requesting stop.
    pub fn into_observer(mut self) -> AsyncOperationObserver {
        let parts = self.parts.take().expect("handle parts are present");
        parts.slot.discard();
        AsyncOperationObserver { core: parts.core }
    }

    fn parts(&self) -> &HandleParts<T, E> {
        self.parts.as_ref().expect("handle parts are present")
    }
}

impl<T: Send + 'static, E: Send + 'static> Drop for OperationHandle<T, E> {
    fn drop(&mut self) {
        let Some(parts) = self.parts.take() else {
            return;
        };
        if parts.core.execution() != ExecutionPhase::Finished {
            if parts.slot.request_stop(StopReason::UserCancelled) {
                parts.stop.request_stop(StopReason::UserCancelled);
            }
        }
        parts.slot.discard();
    }
}

impl<T: Send + 'static, E: Send + 'static> Drop for AsyncOperationHandle<T, E> {
    fn drop(&mut self) {
        let Some(parts) = self.parts.take() else {
            return;
        };
        if parts.core.execution() != ExecutionPhase::Finished {
            if parts.slot.request_stop(StopReason::UserCancelled) {
                parts.stop.request_stop(StopReason::UserCancelled);
            }
        }
        parts.slot.discard();
    }
}

/// Synchronous, untyped read-only lifecycle observer.
#[derive(Clone)]
pub struct OperationObserver {
    core: Arc<OperationCore>,
}

/// Asynchronous, untyped read-only lifecycle observer.
#[derive(Clone)]
pub struct AsyncOperationObserver {
    core: Arc<OperationCore>,
}

impl OperationObserver {
    /// Returns this operation's stable ID.
    pub fn id(&self) -> OperationId {
        self.core.id()
    }

    /// Returns the latest untyped lifecycle snapshot.
    pub fn snapshot(&self) -> OperationSnapshot {
        self.core.snapshot()
    }

    /// Blocks until the stable settlement report is published.
    pub fn wait_settlement(&self) -> Result<Arc<SettlementReport>, ObservationError> {
        if self.core.is_blocking_worker() {
            return Err(ObservationError::WouldDeadlock);
        }
        Ok(self.core.wait_settlement_blocking())
    }

    /// Waits for settlement for at most `timeout`, returning the current snapshot on timeout.
    pub fn wait_settlement_for(
        &self,
        timeout: Duration,
    ) -> Result<SettlementWait, ObservationError> {
        if self.core.is_blocking_worker() {
            return Err(ObservationError::WouldDeadlock);
        }
        Ok(self.core.wait_settlement_for(timeout))
    }

    /// Creates the async view of this read-only observer.
    pub fn as_async(&self) -> AsyncOperationObserver {
        AsyncOperationObserver {
            core: Arc::clone(&self.core),
        }
    }
}

impl AsyncOperationObserver {
    /// Returns this operation's stable ID.
    pub fn id(&self) -> OperationId {
        self.core.id()
    }

    /// Returns the latest untyped lifecycle snapshot.
    pub fn snapshot(&self) -> OperationSnapshot {
        self.core.snapshot()
    }

    /// Asynchronously waits for the immutable settlement report.
    pub async fn wait_settlement(&self) -> Arc<SettlementReport> {
        self.core.wait_settlement_async().await
    }

    /// Waits until the deadline, returning a pending snapshot if settlement is not ready.
    pub async fn wait_settlement_until(
        &self,
        deadline: Deadline,
    ) -> Result<SettlementWait, ObservationError> {
        self.core.wait_settlement_until(deadline).await
    }

    /// Creates the sync view of this read-only observer.
    pub fn as_sync(&self) -> OperationObserver {
        OperationObserver {
            core: Arc::clone(&self.core),
        }
    }
}

struct SettlementFuture<'a> {
    core: &'a OperationCore,
    notification: Option<Notified>,
}

impl Future for SettlementFuture<'_> {
    type Output = Arc<SettlementReport>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let generation = self.core.changed.generation();
            if let Some(report) = self.core.settlement() {
                return Poll::Ready(report);
            }
            if self.notification.is_none() {
                self.notification = Some(self.core.changed.notified_since(generation));
            }
            match Pin::new(self.notification.as_mut().expect("notification was set")).poll(context)
            {
                Poll::Ready(()) => self.notification = None,
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

struct SettlementUntil<'a> {
    core: &'a OperationCore,
    deadline: Deadline,
    notification: Option<Notified>,
    timer: Option<Box<dyn TimerRegistration>>,
}

impl Future for SettlementUntil<'_> {
    type Output = Result<SettlementWait, ObservationError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            if let Some(report) = self.core.settlement() {
                self.cancel_timer();
                return Poll::Ready(Ok(SettlementWait::Ready(report)));
            }
            let Some(clock) = self.core.clock.as_ref() else {
                self.cancel_timer();
                return Poll::Ready(Err(ObservationError::ClockUnavailable));
            };
            let now = clock.now();
            if now.domain() != self.deadline.time_point().domain() {
                self.cancel_timer();
                return Poll::Ready(Err(ObservationError::ClockDomainMismatch));
            }
            if now.ticks() >= self.deadline.time_point().ticks() {
                self.cancel_timer();
                return Poll::Ready(Ok(SettlementWait::Pending(self.core.snapshot())));
            }
            if self.notification.is_none() {
                let generation = self.core.changed.generation();
                if let Some(report) = self.core.settlement() {
                    self.cancel_timer();
                    return Poll::Ready(Ok(SettlementWait::Ready(report)));
                }
                self.notification = Some(self.core.changed.notified_since(generation));
            }
            if self.timer.is_none() {
                self.timer = Some(
                    clock.register_deadline(self.deadline.time_point(), context.waker().clone()),
                );
                // Registration may race the clock reaching its deadline.
                let now = clock.now();
                if now.domain() != self.deadline.time_point().domain() {
                    self.cancel_timer();
                    return Poll::Ready(Err(ObservationError::ClockDomainMismatch));
                }
                if now.ticks() >= self.deadline.time_point().ticks() {
                    self.cancel_timer();
                    return Poll::Ready(Ok(SettlementWait::Pending(self.core.snapshot())));
                }
            }

            match Pin::new(self.notification.as_mut().expect("notification was set")).poll(context)
            {
                Poll::Ready(()) => {
                    self.notification = None;
                    continue;
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl SettlementUntil<'_> {
    fn cancel_timer(&mut self) {
        if let Some(timer) = self.timer.take() {
            timer.cancel();
        }
    }
}

impl Drop for SettlementUntil<'_> {
    fn drop(&mut self) {
        self.cancel_timer();
    }
}

#[cfg(test)]
mod tests;

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

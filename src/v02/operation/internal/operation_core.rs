// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use super::core_facts::CoreFacts;
use super::helpers::lock;
use super::settlement_future::SettlementFuture;
use super::settlement_until::SettlementUntil;
use crate::v02::internal::notify::Notify;
use crate::v02::operation::internal::AVAILABILITY_AVAILABLE;
use crate::v02::operation::internal::AVAILABILITY_DISCARDED;
use crate::v02::operation::internal::AVAILABILITY_PENDING;
use crate::v02::operation::internal::AVAILABILITY_TAKEN;
use crate::v02::protocol::ClockSource;
use crate::v02::protocol::Deadline;
use crate::v02::report::ExecutionPhase;
use crate::v02::report::ExternalProtocolFault;
use crate::v02::report::ExternalProtocolPort;
use crate::v02::report::OperationId;
use crate::v02::report::OperationSnapshot;
use crate::v02::report::OutcomeAvailability;
use crate::v02::report::ResourceProgress;
use crate::v02::report::SettlementReport;
use crate::v02::report::SettlementWait;
use crate::v02::report::StopReport;

/// Untyped lifecycle facts shared with read-only observers.
///
/// The core deliberately stores no typed outcome. The typed outcome slot owns
/// the only typed state, while snapshots expose only availability and reports.
pub(crate) struct OperationCore {
    /// Stable identity assigned when this operation is created.
    pub(in crate::v02::operation) id: OperationId,
    /// Mutex-protected execution, stop, resource, and settlement facts.
    pub(in crate::v02::operation) facts: Mutex<CoreFacts>,
    /// Atomic outcome availability shared with read-only observers.
    pub(in crate::v02::operation) availability: AtomicU8,
    /// Async notification source for operation state changes.
    pub(in crate::v02::operation) changed: Notify,
    /// Blocking notification source for settlement waiters.
    pub(in crate::v02::operation) changed_blocking: Condvar,
    /// Optional clock used to timestamp stop requests and deadlines.
    pub(in crate::v02::operation) clock: Option<Arc<dyn ClockSource>>,
    /// Predicate that identifies workers where blocking waits could deadlock.
    pub(in crate::v02::operation) blocking_worker: Arc<dyn Fn() -> bool + Send + Sync>,
}

impl OperationCore {
    /// Creates an untyped operation core with its initial queued snapshot.
    ///
    /// # Parameters
    ///
    /// - `id`: Identity assigned to the operation.
    ///
    /// # Returns
    ///
    /// A shared operation core with no clock and no blocking-worker context.
    pub(crate) fn new(id: OperationId) -> Arc<Self> {
        Self::new_with_observers(id, None, Arc::new(|| false))
    }

    /// Creates an operation core with optional clock and worker observers.
    ///
    /// # Parameters
    ///
    /// - `id`: Identity assigned to the operation.
    /// - `clock`: Optional clock used for stop timestamps and timed waits.
    /// - `blocking_worker`: Predicate that reports whether the current caller
    ///   runs on a worker that must not block.
    ///
    /// # Returns
    ///
    /// A shared operation core with the queued execution phase.
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
                release_fault: None,
                settlement_fault: None,
            }),
            availability: AtomicU8::new(AVAILABILITY_PENDING),
            changed: Notify::new(),
            changed_blocking: Condvar::new(),
            clock,
            blocking_worker,
        })
    }

    /// Returns this operation's stable identity.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose identity is read.
    ///
    /// # Returns
    ///
    /// The operation identifier.
    #[must_use = "the operation identifier should be used"]
    pub(crate) fn id(&self) -> OperationId {
        self.id
    }

    /// Captures the current immutable operation snapshot.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose state is captured.
    ///
    /// # Returns
    ///
    /// A snapshot of execution, outcome, stop, resource, and settlement state.
    #[must_use = "the operation snapshot should be inspected"]
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

    /// Records the first fault for an external protocol port.
    ///
    /// # Parameters
    ///
    /// - `fault`: Bounded diagnostic to retain if this port has no prior fault.
    pub(crate) fn record_protocol_fault(&self, fault: ExternalProtocolFault) {
        let mut facts = lock(&self.facts);
        let slot = match fault.port {
            ExternalProtocolPort::Release => &mut facts.release_fault,
            ExternalProtocolPort::Settlement => &mut facts.settlement_fault,
        };
        if slot.is_none() {
            *slot = Some(fault);
            drop(facts);
            self.notify_change();
        }
    }

    /// Returns the recorded external protocol faults in stable port order.
    pub(crate) fn external_protocol_faults(&self) -> Arc<[ExternalProtocolFault]> {
        let faults = {
            let facts = lock(&self.facts);
            let mut faults = Vec::with_capacity(2);
            if let Some(fault) = &facts.release_fault {
                faults.push(fault.clone());
            }
            if let Some(fault) = &facts.settlement_fault {
                faults.push(fault.clone());
            }
            faults
        };
        Arc::from(faults)
    }

    /// Reads the outcome availability marker for observers.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose outcome marker is read.
    ///
    /// # Returns
    ///
    /// The public availability state corresponding to the atomic marker.
    #[must_use = "the outcome availability should be inspected"]
    pub(in crate::v02::operation) fn outcome_availability(&self) -> OutcomeAvailability {
        match self.availability.load(Ordering::Acquire) {
            AVAILABILITY_AVAILABLE => OutcomeAvailability::Available,
            AVAILABILITY_TAKEN => OutcomeAvailability::Taken,
            AVAILABILITY_DISCARDED => OutcomeAvailability::Discarded,
            _ => OutcomeAvailability::Pending,
        }
    }

    /// Returns the current execution phase.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose execution phase is read.
    ///
    /// # Returns
    ///
    /// The current execution phase.
    #[must_use = "the execution phase should be inspected"]
    pub(crate) fn execution(&self) -> ExecutionPhase {
        lock(&self.facts).execution
    }

    /// Updates the execution phase and notifies observers.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose phase is updated.
    /// - `execution`: New execution phase to publish.
    pub(crate) fn set_execution(&self, execution: ExecutionPhase) {
        lock(&self.facts).execution = execution;
        self.notify_change();
    }

    /// Updates the resource fact without waking observers.
    ///
    /// Scope callers use this while serializing publication with terminal
    /// resource events, then notify after releasing the scope-state lock.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose resource progress fact is updated.
    /// - `resources`: Current resource progress, or `None` when unavailable.
    pub(crate) fn update_resource_progress(&self, resources: Option<ResourceProgress>) {
        lock(&self.facts).resources = resources;
    }

    /// Marks an outcome available unless a terminal availability state won.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose outcome is published.
    pub(in crate::v02::operation) fn mark_available(&self) {
        let _ = self.availability.compare_exchange(
            AVAILABILITY_PENDING,
            AVAILABILITY_AVAILABLE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.notify_change();
    }

    /// Marks the outcome as taken and notifies observers.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose outcome was taken.
    pub(in crate::v02::operation) fn mark_taken(&self) {
        self.availability.store(AVAILABILITY_TAKEN, Ordering::Release);
        self.notify_change();
    }

    /// Marks the outcome as discarded and notifies observers.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose outcome was discarded.
    pub(in crate::v02::operation) fn mark_discarded(&self) {
        self.availability.store(AVAILABILITY_DISCARDED, Ordering::Release);
        self.notify_change();
    }

    /// Records the first accepted stop report and notifies observers.
    ///
    /// Later reports do not replace the first accepted request.
    ///
    /// # Parameters
    ///
    /// - `self`: The core receiving the stop report.
    /// - `stop`: Accepted stop reason and optional acceptance time.
    pub(in crate::v02::operation) fn record_stop(&self, stop: StopReport) {
        let mut facts = lock(&self.facts);
        if facts.stop.is_none() {
            facts.stop = Some(stop);
        }
        drop(facts);
        self.notify_change();
    }

    /// Publishes exactly one immutable settlement snapshot.
    ///
    /// # Parameters
    ///
    /// - `self`: The core that owns the settlement state.
    /// - `report`: Immutable settlement report to publish.
    ///
    /// # Returns
    ///
    /// The published shared report, or the unchanged report if settlement was
    /// already published.
    ///
    /// # Errors
    ///
    /// Returns the input report when another caller has already published
    /// settlement.
    pub(crate) fn publish_settlement(
        &self,
        report: SettlementReport,
    ) -> Result<Arc<SettlementReport>, Box<SettlementReport>> {
        let mut facts = lock(&self.facts);
        if facts.settlement.is_some() {
            drop(facts);
            return Err(Box::new(report));
        }
        let report = Arc::new(report);
        facts.settlement = Some(Arc::clone(&report));
        facts.resources = None;
        drop(facts);
        self.notify_change();
        Ok(report)
    }

    /// Clones the settlement report when one has been published.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose settlement is queried.
    ///
    /// # Returns
    ///
    /// The shared report if settlement is complete, or `None` while settlement
    /// has not yet been published.
    pub(in crate::v02::operation) fn settlement(&self) -> Option<Arc<SettlementReport>> {
        lock(&self.facts).settlement.as_ref().map(Arc::clone)
    }

    /// Wakes asynchronous and blocking waiters after state changes.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose waiters are notified.
    pub(crate) fn notify_change(&self) {
        self.changed.notify_waiters();
        self.changed_blocking.notify_all();
    }

    /// Blocks until a settlement report has been published.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose settlement is awaited.
    ///
    /// # Returns
    ///
    /// The shared settlement report.
    pub(in crate::v02::operation) fn wait_settlement_blocking(&self) -> Arc<SettlementReport> {
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

    /// Waits for settlement up to the given duration.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose settlement is awaited.
    /// - `timeout`: Maximum time to wait using the process monotonic clock.
    ///
    /// # Returns
    ///
    /// A ready settlement report or a pending snapshot after timeout.
    pub(in crate::v02::operation) fn wait_settlement_for(&self, timeout: Duration) -> SettlementWait {
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

    /// Tests whether the caller is running on a blocking-sensitive worker.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose worker predicate is evaluated.
    ///
    /// # Returns
    ///
    /// `true` when the current worker must not perform blocking waits.
    #[must_use = "the blocking-worker state should be checked before waiting"]
    pub(in crate::v02::operation) fn is_blocking_worker(&self) -> bool {
        (self.blocking_worker)()
    }

    /// Creates a future that waits for settlement without blocking.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose settlement is awaited.
    ///
    /// # Returns
    ///
    /// A future resolving to the shared settlement report.
    pub(in crate::v02::operation) fn wait_settlement_async(&self) -> SettlementFuture<'_> {
        SettlementFuture {
            core: self,
            notification: None,
        }
    }

    /// Creates a future that waits for settlement until a deadline.
    ///
    /// # Parameters
    ///
    /// - `self`: The core whose settlement is awaited.
    /// - `deadline`: Deadline that bounds the wait.
    ///
    /// # Returns
    ///
    /// A future resolving to settlement, a pending snapshot, or a clock error.
    pub(in crate::v02::operation) fn wait_settlement_until(&self, deadline: Deadline) -> SettlementUntil<'_> {
        SettlementUntil {
            core: self,
            deadline,
            notification: None,
            timer: None,
            registered_waker: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use super::OperationCore;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::ExternalProtocolFault;
    use crate::v02::report::ExternalProtocolFaultKind;
    use crate::v02::report::ExternalProtocolPort;
    use crate::v02::report::OperationId;
    use crate::v02::report::SettlementReport;

    #[test]
    fn test_external_protocol_fault_slots_are_bounded_ordered_and_first_wins() {
        let id = OperationId::new_unique();
        let core = OperationCore::new(id);
        assert!(core.external_protocol_faults().is_empty());
        let fault = |port, summary: &'static str| ExternalProtocolFault {
            operation_id: id,
            port,
            kind: ExternalProtocolFaultKind::IdentityMismatch,
            observed_operation_id: None,
            summary: DiagnosticSummary::new(summary),
        };
        core.record_protocol_fault(fault(ExternalProtocolPort::Settlement, "settlement first"));
        core.record_protocol_fault(fault(ExternalProtocolPort::Settlement, "settlement second"));
        core.record_protocol_fault(fault(ExternalProtocolPort::Release, "release first"));
        core.record_protocol_fault(fault(ExternalProtocolPort::Release, "release second"));
        let faults = core.external_protocol_faults();
        assert_eq!(faults.len(), 2);
        assert_eq!(faults[0].port, ExternalProtocolPort::Release);
        assert_eq!(faults[0].summary.as_str(), "release first");
        assert_eq!(faults[1].port, ExternalProtocolPort::Settlement);
        assert_eq!(faults[1].summary.as_str(), "settlement first");
    }
    use crate::v02::report::SettlementStatus;
    use crate::v02::report::SettlementWait;

    fn report(id: OperationId) -> SettlementReport {
        SettlementReport {
            operation_id: id,
            status: SettlementStatus::Released,
            released_count: 0,
            transferred_count: 0,
            recovery_count: 0,
            children: Arc::from([]),
            children_total: 0,
            children_truncated: false,
            cleanup_failures: Arc::from([]),
            cleanup_failures_total: 0,
            cleanup_failures_truncated: false,
            resource_reports: Arc::from([]),
            resource_reports_total: 0,
            resource_reports_truncated: false,
            transfers: Arc::from([]),
            transfers_total: 0,
            transfers_truncated: false,
            recoveries: Arc::from([]),
            recoveries_total: 0,
            recoveries_truncated: false,
            effects: Arc::from([]),
            effects_total: 0,
            effect_overflow: 0,
            external_settlement: None,
        }
    }

    #[test]
    fn test_blocking_settlement_wait_returns_published_report() {
        let id = OperationId::new_unique();
        let core = OperationCore::new(id);
        let expected = core.publish_settlement(report(id)).unwrap();
        let actual = core.wait_settlement_blocking();
        assert!(Arc::ptr_eq(&actual, &expected));
    }

    #[test]
    fn test_timed_settlement_wait_returns_pending_on_timeout() {
        let core = OperationCore::new(OperationId::new_unique());
        assert!(matches!(
            core.wait_settlement_for(Duration::from_millis(1)),
            SettlementWait::Pending(snapshot) if snapshot.settlement.is_none()
        ));
    }

    #[test]
    fn test_overflowing_settlement_timeout_waits_until_report_is_published() {
        let id = OperationId::new_unique();
        let core = OperationCore::new(id);
        let publisher = Arc::clone(&core);
        let thread = thread::spawn(move || {
            thread::sleep(Duration::from_millis(2));
            publisher.publish_settlement(report(id)).unwrap();
        });

        assert!(matches!(
            core.wait_settlement_for(Duration::MAX),
            SettlementWait::Ready(report) if report.operation_id == id
        ));
        thread.join().unwrap();
    }
}

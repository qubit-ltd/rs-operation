use super::*;
use crate::v02::protocol::{
    ClockDomainId, ClockSource, Deadline, MonotonicTime, TimerRegistration,
};
use crate::v02::report::{
    OperationId, OperationOutcome, ResourceProgress, SettlementReport, SettlementStatus, StopReason,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

fn settlement(operation_id: OperationId, status: SettlementStatus) -> SettlementReport {
    SettlementReport {
        operation_id,
        status,
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
    }
}

fn poll_once<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let mut context = Context::from_waker(Waker::noop());
    future.poll(&mut context)
}

fn handle<T: Send + 'static, E: Send + 'static>() -> (OperationHandle<T, E>, Arc<OutcomeSlot<T, E>>)
{
    let core = OperationCore::new(OperationId::new_unique());
    let slot = Arc::new(OutcomeSlot::new(Arc::clone(&core)));
    let handle = OperationHandle::new(Arc::clone(&core), Arc::clone(&slot), Arc::new(NoopStop));
    (handle, slot)
}

fn async_handle<T: Send + 'static, E: Send + 'static>()
-> (AsyncOperationHandle<T, E>, Arc<OutcomeSlot<T, E>>) {
    let core = OperationCore::new(OperationId::new_unique());
    let slot = Arc::new(OutcomeSlot::new(Arc::clone(&core)));
    let handle =
        AsyncOperationHandle::new(Arc::clone(&core), Arc::clone(&slot), Arc::new(NoopStop));
    (handle, slot)
}

struct NoopStop;

impl StopControl for NoopStop {
    fn request_stop(&self, _reason: StopReason) {}
}

#[test]
fn take_outcome_exactly_once() {
    let (mut handle, slot) = handle::<u32, ()>();
    assert!(slot.publish(OperationOutcome::Succeeded(73)).is_ok());
    assert!(matches!(
        handle.try_take_outcome(),
        Ok(Some(OperationOutcome::Succeeded(73)))
    ));
    assert_eq!(
        handle.try_take_outcome().unwrap_err(),
        crate::v02::report::OutcomeError::AlreadyTaken
    );
}

#[test]
fn cancel_wait_does_not_take_outcome() {
    let (mut handle, slot) = async_handle::<u32, ()>();
    let mut wait = Box::pin(handle.wait_outcome());
    assert!(poll_once(wait.as_mut()).is_pending());
    drop(wait);

    assert!(slot.publish(OperationOutcome::Succeeded(91)).is_ok());
    let mut retry = Box::pin(handle.wait_outcome());
    assert!(matches!(
        poll_once(retry.as_mut()),
        Poll::Ready(Ok(OperationOutcome::Succeeded(91)))
    ));
}

#[test]
fn observer_does_not_keep_typed_result() {
    struct DropCount(Arc<AtomicUsize>);
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let (handle, slot) = handle::<DropCount, ()>();
    assert!(
        slot.publish(OperationOutcome::Succeeded(DropCount(Arc::clone(&drops))))
            .is_ok()
    );
    let observer = handle.into_observer();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(observer.snapshot().outcome, OutcomeAvailability::Discarded);
    drop(observer);
}

#[test]
fn into_async_moves_delivery_without_stop() {
    let (handle, slot) = handle::<u32, ()>();
    assert!(slot.publish(OperationOutcome::Succeeded(8)).is_ok());
    let mut handle = handle.into_async();
    assert_eq!(handle.snapshot().stop, None);
    let mut wait = Box::pin(handle.wait_outcome());
    assert!(matches!(
        poll_once(wait.as_mut()),
        Poll::Ready(Ok(OperationOutcome::Succeeded(8)))
    ));
}

#[test]
fn into_sync_moves_delivery_without_stop() {
    let (handle, slot) = async_handle::<u32, ()>();
    assert!(slot.publish(OperationOutcome::Succeeded(14)).is_ok());
    let mut handle = handle.into_sync();
    assert_eq!(handle.snapshot().stop, None);
    assert!(matches!(
        handle.try_take_outcome(),
        Ok(Some(OperationOutcome::Succeeded(14)))
    ));
}

#[test]
fn into_observer_discards_without_stop() {
    let (handle, slot) = handle::<u32, ()>();
    assert!(slot.publish(OperationOutcome::Succeeded(8)).is_ok());
    let observer = handle.into_observer();
    assert_eq!(observer.snapshot().outcome, OutcomeAvailability::Discarded);
    assert_eq!(observer.snapshot().stop, None);
}

#[test]
fn unfinished_handle_drop_requests_stop() {
    let (handle, _) = handle::<u32, ()>();
    let observer = handle.observer();
    drop(handle);
    assert_eq!(
        observer
            .snapshot()
            .stop
            .as_ref()
            .map(|report| report.reason),
        Some(StopReason::UserCancelled)
    );
    assert_eq!(observer.snapshot().outcome, OutcomeAvailability::Discarded);
}

#[test]
fn settlement_is_published_once_and_stays_immutable() {
    let (handle, _) = handle::<u32, ()>();
    let id = handle.id();
    handle.parts().core.set_execution(ExecutionPhase::Running);
    let first = handle
        .parts()
        .core
        .publish_settlement(settlement(id, SettlementStatus::Transferred))
        .unwrap();
    let second_candidate = settlement(id, SettlementStatus::Released);
    assert!(
        handle
            .parts()
            .core
            .publish_settlement(second_candidate)
            .is_err()
    );
    assert!(Arc::ptr_eq(&first, &handle.snapshot().settlement.unwrap()));
    assert_eq!(first.status, SettlementStatus::Transferred);
}

#[test]
fn outcome_and_settlement_are_independent_facts() {
    let (handle, slot) = handle::<u32, ()>();
    let id = handle.id();
    handle
        .parts()
        .core
        .publish_settlement(settlement(id, SettlementStatus::Released))
        .unwrap();
    assert_eq!(handle.snapshot().outcome, OutcomeAvailability::Pending);

    assert!(slot.publish(OperationOutcome::Failed(())).is_ok());
    assert_eq!(handle.snapshot().outcome, OutcomeAvailability::Available);
    assert_eq!(
        handle.snapshot().settlement.unwrap().status,
        SettlementStatus::Released
    );
}

#[test]
fn operation_types_do_not_require_clone_or_debug() {
    struct Opaque;

    let (mut handle, slot) = handle::<Opaque, Opaque>();
    assert!(slot.publish(OperationOutcome::Succeeded(Opaque)).is_ok());
    assert!(matches!(
        handle.try_take_outcome(),
        Ok(Some(OperationOutcome::Succeeded(Opaque)))
    ));
}

#[test]
fn observer_conversion_and_resource_progress_keep_the_untyped_core() {
    let (handle, _) = handle::<u32, ()>();
    let observer = handle.observer();
    let async_observer = observer.as_async();
    let sync_observer = async_observer.as_sync();
    assert_eq!(observer.id(), sync_observer.id());

    handle
        .parts()
        .core
        .set_resource_progress(Some(ResourceProgress::WaitingForUsers));
    assert_eq!(
        observer.snapshot().resources,
        Some(ResourceProgress::WaitingForUsers)
    );
}

#[test]
fn bounded_settlement_wait_returns_pending_snapshot() {
    let (handle, _) = handle::<u32, ()>();
    let observer = handle.observer();
    assert!(matches!(
        observer.wait_settlement_for(Duration::ZERO).unwrap(),
        SettlementWait::Pending(snapshot) if snapshot.settlement.is_none()
    ));
}

#[test]
fn async_settlement_deadline_only_limits_observation() {
    struct FixedClock(MonotonicTime);

    impl ClockSource for FixedClock {
        fn now(&self) -> MonotonicTime {
            self.0
        }

        fn register_deadline(
            &self,
            _at: MonotonicTime,
            _wake: Waker,
        ) -> Box<dyn TimerRegistration> {
            Box::new(NoopTimer)
        }
    }

    struct NoopTimer;

    impl TimerRegistration for NoopTimer {
        fn cancel(self: Box<Self>) {}
    }

    let domain = ClockDomainId::new_unique();
    let now = MonotonicTime::new(domain, 10);
    let core = OperationCore::new_with_observers(
        OperationId::new_unique(),
        Some(Arc::new(FixedClock(now))),
        Arc::new(|| false),
    );
    let slot = Arc::new(OutcomeSlot::<u32, ()>::new(Arc::clone(&core)));
    let handle = AsyncOperationHandle::new(Arc::clone(&core), slot, Arc::new(NoopStop));
    let observer = handle.observer();
    let mut wait = Box::pin(observer.wait_settlement_until(Deadline::at(now)));
    assert!(matches!(
        poll_once(wait.as_mut()),
        Poll::Ready(Ok(SettlementWait::Pending(snapshot))) if snapshot.stop.is_none()
    ));
}

#[test]
fn outcome_delivery_hook_notifies_take_and_discard_once() {
    let core = OperationCore::new(OperationId::new_unique());
    let slot = OutcomeSlot::<u8, ()>::new(Arc::clone(&core));
    let deliveries = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = Arc::clone(&deliveries);
    slot.set_delivery_hook(move |delivery| observed.lock().unwrap().push(delivery));
    slot.publish(OperationOutcome::Succeeded(7)).unwrap();
    assert!(matches!(
        slot.take_if_ready(),
        Ok(Some(OperationOutcome::Succeeded(7)))
    ));
    slot.discard();
    assert_eq!(
        *deliveries.lock().unwrap(),
        vec![OutcomeDelivery::Delivered]
    );

    let discarded = OutcomeSlot::<u8, ()>::new(OperationCore::new(OperationId::new_unique()));
    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = Arc::clone(&events);
    discarded.set_delivery_hook(move |delivery| observed.lock().unwrap().push(delivery));
    discarded.discard();
    let rejected = discarded
        .publish(OperationOutcome::Succeeded(9))
        .expect_err("discarded result remains unavailable");
    assert!(matches!(rejected, OperationOutcome::Succeeded(9)));
    assert_eq!(*events.lock().unwrap(), vec![OutcomeDelivery::Discarded]);
}

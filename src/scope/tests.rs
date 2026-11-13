// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests that require access to private scope waiting state.

use std::future::Future;
use std::future::ready;
use std::mem::forget;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc::channel;
use std::task::Context;
use std::task::Poll;
use std::task::RawWaker;
use std::task::RawWakerVTable;
use std::task::Waker;
use std::thread::spawn;
use std::time::Duration;

use super::ChildRequirement;
use super::JoinFacts;
use super::OperationScope;
use super::ScopeError;
use super::ScopePolicy;
use crate::context::ContextOptions;
use crate::context::OperationContext;
use crate::protocol::CleanupContext;
use crate::protocol::ManagedOperationControl;
use crate::protocol::OwnedCleanup;
use crate::report::ChildBusinessOutcome;
use crate::report::ChildBusinessSummary;
use crate::report::ChildSettlement;
use crate::report::ChildSnapshot;
use crate::report::CleanupOutcome;
use crate::report::DiagnosticSummary;
use crate::report::EffectFact;
use crate::report::EffectRecord;
use crate::report::OperationId;
use crate::report::OperationPhase;
use crate::report::ResourcePhase;
use crate::report::ScopeLimits;
use crate::report::StopReason;
use crate::resource::CleanupScheduler;
use crate::resource::ResourceRegistry;

struct FixedControl {
    operation_id: OperationId,
    business: std::sync::Mutex<Poll<ChildBusinessSummary>>,
    released: std::sync::Mutex<Poll<ChildSettlement>>,
    stop: std::sync::Mutex<Option<StopReason>>,
}

impl ManagedOperationControl for FixedControl {
    fn request_stop(&self, reason: StopReason) {
        *self.stop.lock().unwrap() = Some(reason);
    }

    fn snapshot(&self) -> ChildSnapshot {
        ChildSnapshot {
            operation_id: self.operation_id,
            phase: OperationPhase::ResultReady,
            resource_phase: ResourcePhase::Active,
            stop_requested: self.stop.lock().unwrap().is_some(),
            released: false,
        }
    }

    fn poll_business(&self, _: &mut Context<'_>) -> Poll<ChildBusinessSummary> {
        self.business.lock().unwrap().clone()
    }

    fn poll_released(&self, _: &mut Context<'_>) -> Poll<ChildSettlement> {
        self.released.lock().unwrap().clone()
    }
}

fn fixed_control(operation_id: OperationId, outcome: ChildBusinessOutcome) -> Arc<FixedControl> {
    Arc::new(FixedControl {
        operation_id,
        business: std::sync::Mutex::new(Poll::Ready(ChildBusinessSummary {
            operation_id,
            outcome,
            summary: DiagnosticSummary::new("child outcome"),
        })),
        released: std::sync::Mutex::new(Poll::Ready(ChildSettlement {
            operation_id,
            released: true,
            summary: DiagnosticSummary::new("child released"),
        })),
        stop: std::sync::Mutex::new(None),
    })
}

fn test_scope(policy: ScopePolicy, child_operations: usize) -> OperationScope {
    let limits = ScopeLimits {
        child_operations,
        ..ScopeLimits::default()
    };
    OperationScope::new(
        OperationId::new(1),
        limits,
        policy,
        CleanupScheduler::new(1),
        OperationContext::new(ContextOptions::default()),
    )
}

struct ReadyCleanup;

#[test]
fn test_unattached_child_reservation_returns_error_and_releases_capacity() {
    let scope = test_scope(ScopePolicy::Collect, 1);
    let reservation = scope.reserve_child(ChildRequirement::Required).unwrap();

    assert!(matches!(reservation.commit(), Err(ScopeError::NotAttached)));
    assert!(scope.reserve_child(ChildRequirement::Optional).is_ok());
}

#[test]
fn test_child_reservation_rejects_second_control_attachment() {
    let scope = test_scope(ScopePolicy::Collect, 1);
    let mut reservation = scope.reserve_child(ChildRequirement::Required).unwrap();
    let control = fixed_control(OperationId::new(2), ChildBusinessOutcome::Succeeded);

    reservation.attach(control.clone()).unwrap();
    assert!(matches!(reservation.attach(control), Err(ScopeError::AlreadyAttached)));
}

#[test]
fn test_child_reservation_commit_rejects_cancelled_and_closed_scopes() {
    let cancelled = test_scope(ScopePolicy::Collect, 1);
    let mut reservation = cancelled.reserve_child(ChildRequirement::Required).unwrap();
    reservation
        .attach(fixed_control(OperationId::new(5), ChildBusinessOutcome::Succeeded))
        .unwrap();
    cancelled.request_cancel(StopReason::UserCancelled);
    assert!(matches!(reservation.commit(), Err(ScopeError::Cancelled)));

    let closed = test_scope(ScopePolicy::Collect, 1);
    let mut reservation = closed.reserve_child(ChildRequirement::Required).unwrap();
    reservation
        .attach(fixed_control(OperationId::new(6), ChildBusinessOutcome::Succeeded))
        .unwrap();
    closed.close_body();
    assert!(matches!(reservation.commit(), Err(ScopeError::Closed)));
}

#[test]
fn test_required_child_failure_is_reported_once_and_requests_fail_fast_stop() {
    let scope = test_scope(ScopePolicy::FailFast, 1);
    let control = fixed_control(OperationId::new(3), ChildBusinessOutcome::Failed);
    let mut reservation = scope.reserve_child(ChildRequirement::Required).unwrap();
    reservation.attach(control.clone()).unwrap();
    reservation.commit().unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut facts = JoinFacts::default();

    assert!(super::internal::poll::poll_child_business(&scope, &mut cx, &mut facts).is_ready());
    assert_eq!(
        facts.failed_required.as_ref().unwrap().operation_id,
        OperationId::new(3)
    );
    assert_eq!(*control.stop.lock().unwrap(), Some(StopReason::RequiredChildFailed));
    assert!(super::internal::poll::poll_child_business(&scope, &mut cx, &mut facts).is_ready());
    assert_eq!(facts.business.len(), 1);
    assert!(super::internal::poll::poll_child_releases(&scope, &mut cx, &mut facts).is_ready());
    assert_eq!(facts.settlements.len(), 1);
}

#[test]
fn test_optional_child_failure_does_not_request_scope_stop() {
    let scope = test_scope(ScopePolicy::Collect, 1);
    let control = fixed_control(OperationId::new(4), ChildBusinessOutcome::Panicked);
    let mut reservation = scope.reserve_child(ChildRequirement::Optional).unwrap();
    reservation.attach(control.clone()).unwrap();
    reservation.commit().unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut facts = JoinFacts::default();

    assert!(super::internal::poll::poll_child_business(&scope, &mut cx, &mut facts).is_ready());
    assert!(facts.failed_required.is_none());
    assert_eq!(*control.stop.lock().unwrap(), None);
}

#[test]
fn test_scope_polling_waits_for_business_and_release_facts() {
    let scope = test_scope(ScopePolicy::Collect, 1);
    let control = fixed_control(OperationId::new(7), ChildBusinessOutcome::Succeeded);
    *control.business.lock().unwrap() = Poll::Pending;
    *control.released.lock().unwrap() = Poll::Pending;
    let mut reservation = scope.reserve_child(ChildRequirement::Required).unwrap();
    reservation.attach(control.clone()).unwrap();
    reservation.commit().unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut facts = JoinFacts::default();

    assert!(super::internal::poll::poll_scope_business(&scope, &mut cx, &mut facts).is_pending());
    *control.business.lock().unwrap() = Poll::Ready(ChildBusinessSummary {
        operation_id: control.operation_id,
        outcome: ChildBusinessOutcome::Succeeded,
        summary: DiagnosticSummary::new("done"),
    });
    assert!(super::internal::poll::poll_scope_business(&scope, &mut cx, &mut facts).is_ready());
    assert!(super::internal::poll::poll_scope_releases(&scope, &mut cx, &mut facts).is_pending());
    *control.released.lock().unwrap() = Poll::Ready(ChildSettlement {
        operation_id: control.operation_id,
        released: true,
        summary: DiagnosticSummary::new("released"),
    });
    assert!(super::internal::poll::poll_scope_releases(&scope, &mut cx, &mut facts).is_ready());
    assert_eq!(facts.business.len(), 1);
    assert_eq!(facts.settlements.len(), 1);
}

#[test]
fn test_child_scope_enforces_limits_and_closes_with_parent_body() {
    let scope = test_scope(ScopePolicy::Collect, 1);
    let mut too_large = super::ScopeOptions::default();
    too_large.limits.child_operations = 2;

    assert!(matches!(scope.child_scope(too_large), Err(ScopeError::LimitReached)));

    let mut allowed = super::ScopeOptions::default();
    allowed.limits.child_operations = 1;
    let child = scope.child_scope(allowed.clone()).unwrap();
    assert_eq!(scope.child_scopes().len(), 1);
    scope.close_body();
    assert!(matches!(scope.child_scope(allowed), Err(ScopeError::Closed)));
    assert!(matches!(
        child.reserve_child(ChildRequirement::Required),
        Err(ScopeError::Closed)
    ));
}

#[test]
fn test_record_effect_reports_overflow_after_retaining_configured_limit() {
    let limits = ScopeLimits {
        effect_records: 1,
        ..ScopeLimits::default()
    };
    let scope = OperationScope::new(
        OperationId::new(1),
        limits,
        ScopePolicy::Collect,
        CleanupScheduler::new(1),
        OperationContext::new(ContextOptions::default()),
    );
    let make_effect = |description: &str| EffectRecord {
        operation_id: OperationId::new(1),
        resource_id: None,
        description: DiagnosticSummary::new(description),
        fact: EffectFact::Known,
    };

    scope.record_effect(make_effect("first")).unwrap();
    assert!(matches!(
        scope.record_effect(make_effect("overflow")),
        Err(ScopeError::EffectLimitReached)
    ));
    let (records, overflow) = scope.effect_snapshot();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].description.as_str(), "first");
    assert_eq!(overflow, 1);
}

impl OwnedCleanup for ReadyCleanup {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        Box::pin(ready(CleanupOutcome::Cleaned))
    }
}

struct ReentrantProbe {
    registry: ResourceRegistry,
    lock_timeouts: Arc<AtomicUsize>,
}

fn check_registry_reentry(probe: &ReentrantProbe) {
    let registry = probe.registry.clone();
    let (tx, rx) = channel();
    spawn(move || {
        registry.register_change_waker(Waker::noop());
        let _ = tx.send(());
    });
    if rx.recv_timeout(Duration::from_millis(100)).is_err() {
        probe.lock_timeouts.fetch_add(1, Ordering::SeqCst);
    }
}

unsafe fn clone_reentrant_waker(data: *const ()) -> RawWaker {
    let probe = unsafe { Arc::<ReentrantProbe>::from_raw(data.cast()) };
    check_registry_reentry(&probe);
    let clone = Arc::clone(&probe);
    forget(probe);
    RawWaker::new(Arc::into_raw(clone).cast(), &REENTRANT_VTABLE)
}

unsafe fn wake_reentrant_waker(data: *const ()) {
    let probe = unsafe { Arc::<ReentrantProbe>::from_raw(data.cast()) };
    check_registry_reentry(&probe);
    drop(probe);
}

unsafe fn wake_reentrant_waker_by_ref(data: *const ()) {
    let probe = unsafe { &*data.cast::<ReentrantProbe>() };
    check_registry_reentry(probe);
}

unsafe fn drop_reentrant_waker(data: *const ()) {
    let probe = unsafe { Arc::<ReentrantProbe>::from_raw(data.cast()) };
    check_registry_reentry(&probe);
    drop(probe);
}

static REENTRANT_VTABLE: RawWakerVTable = RawWakerVTable::new(
    clone_reentrant_waker,
    wake_reentrant_waker,
    wake_reentrant_waker_by_ref,
    drop_reentrant_waker,
);

#[test]
fn test_resource_waiter_waker_callbacks_can_reenter_registry_without_lock() {
    let scope = OperationScope::new(
        OperationId::new(12),
        ScopeLimits::default(),
        ScopePolicy::FailFast,
        CleanupScheduler::new(1),
        OperationContext::new(ContextOptions::default()),
    );
    let id = scope.register_resource(ReadyCleanup).unwrap();
    let use_lease = scope.acquire_use(id).unwrap();
    scope.close_for_use(id).unwrap();
    let seen = scope.resource_epoch();
    let mut wait = Box::pin(scope.wait_for_resource_change(seen));
    let lock_timeouts = Arc::new(AtomicUsize::new(0));
    let probe = Arc::new(ReentrantProbe {
        registry: scope.resources.clone(),
        lock_timeouts: Arc::clone(&lock_timeouts),
    });
    let raw = RawWaker::new(Arc::into_raw(probe).cast(), &REENTRANT_VTABLE);
    let waker = unsafe { Waker::from_raw(raw) };
    let mut cx = Context::from_waker(&waker);
    assert!(
        wait.as_mut().poll(&mut cx).is_pending(),
        "the waiter should remain pending while the resource use is active"
    );
    drop(use_lease);
    assert!(
        matches!(wait.as_mut().poll(&mut cx), Poll::Ready(())),
        "the waiter should complete after the final resource use is dropped"
    );
    assert_eq!(
        lock_timeouts.load(Ordering::SeqCst),
        0,
        "waker callbacks should reenter the registry without waiting on its lock"
    );
}

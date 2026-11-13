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

use super::OperationScope;
use super::ScopePolicy;
use crate::context::ContextOptions;
use crate::context::OperationContext;
use crate::protocol::CleanupContext;
use crate::protocol::OwnedCleanup;
use crate::report::CleanupOutcome;
use crate::report::OperationId;
use crate::report::ScopeLimits;
use crate::resource::CleanupScheduler;
use crate::resource::ResourceRegistry;

struct ReadyCleanup;

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

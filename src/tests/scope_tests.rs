// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Scope tests for resource-use waiter notifications.

use std::future::Future;
use std::future::ready;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use crate::context::ContextOptions;
use crate::context::OperationContext;
use crate::protocol::CleanupContext;
use crate::protocol::OwnedCleanup;
use crate::report::CleanupOutcome;
use crate::report::OperationId;
use crate::report::ScopeLimits;
use crate::resource::CleanupScheduler;
use crate::scope::OperationScope;
use crate::scope::ScopePolicy;

struct ReadyCleanup;

impl OwnedCleanup for ReadyCleanup {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        Box::pin(ready(CleanupOutcome::Cleaned))
    }
}

struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn test_dropping_last_resource_use_wakes_registered_scope_waiter() {
    let scope = OperationScope::new(
        OperationId::new(1),
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
    let wake_count = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wake_count));
    let mut cx = Context::from_waker(&waker);
    assert!(
        wait.as_mut().poll(&mut cx).is_pending(),
        "the waiter should remain pending while the resource use is active"
    );
    drop(use_lease);
    assert!(
        wake_count.0.load(Ordering::SeqCst) > 0,
        "dropping the final use should wake the registered waiter"
    );
    assert!(
        matches!(wait.as_mut().poll(&mut cx), Poll::Ready(())),
        "the waiter should complete after the final resource use is dropped"
    );
}

#[test]
fn test_opposing_scope_resource_transfers_do_not_invert_handle_locks() {
    let make_scope = |id| {
        OperationScope::new(
            OperationId::new(id),
            ScopeLimits::default(),
            ScopePolicy::FailFast,
            CleanupScheduler::new(1),
            OperationContext::new(ContextOptions::default()),
        )
    };
    let first = make_scope(10);
    let second = make_scope(11);
    let first_resource = first.register_resource(ReadyCleanup).unwrap();
    let second_resource = second.register_resource(ReadyCleanup).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let (tx, rx) = std::sync::mpsc::channel();

    let first_scope = first.clone();
    let first_destination = second.clone();
    let first_barrier = Arc::clone(&barrier);
    let first_tx = tx.clone();
    std::thread::spawn(move || {
        first_barrier.wait();
        first_tx
            .send(first_scope.transfer_resource_to(first_resource, &first_destination))
            .unwrap();
    });

    let second_scope = second.clone();
    let second_destination = first.clone();
    let second_barrier = Arc::clone(&barrier);
    std::thread::spawn(move || {
        second_barrier.wait();
        tx.send(second_scope.transfer_resource_to(second_resource, &second_destination))
            .unwrap();
    });

    barrier.wait();
    let first_result = rx.recv_timeout(std::time::Duration::from_secs(1));
    let second_result = rx.recv_timeout(std::time::Duration::from_secs(1));
    assert!(first_result.is_ok(), "first opposing transfer must complete");
    assert!(second_result.is_ok(), "second opposing transfer must complete");
    assert!(first_result.unwrap().is_ok());
    assert!(second_result.unwrap().is_ok());
}

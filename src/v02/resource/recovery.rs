// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Strongly retained recovery payloads and single-flight retry state.

mod async_recovery_attempt;
pub(super) mod direct_recovery_driver;
mod internal;
mod recovery_attempt;
mod recovery_handle;
mod recovery_store;
mod recovery_store_shutdown_error;
mod recovery_store_shutdown_handle;

pub use async_recovery_attempt::AsyncRecoveryAttempt;
pub(in crate::v02::resource::recovery) use internal::AttemptObservation;
// Preserve the crate-internal recovery protocol paths used by resource owners.
pub(crate) use internal::RecoveryAction;
pub(crate) use internal::RecoveryCore;
pub(crate) use internal::RecoveryFuture;
pub(crate) use internal::RecoveryPayload;
pub(in crate::v02::resource::recovery) use internal::RecoveryRun;
pub(crate) use internal::RecoveryStoreError;
pub(in crate::v02::resource::recovery) use internal::RecoveryStoreShutdownState;
pub(in crate::v02::resource::recovery) use internal::RecoveryStoreState;
pub(crate) use internal::async_owner;
pub(in crate::v02::resource::recovery) use internal::block_on;
pub(in crate::v02::resource::recovery) use internal::lock;
pub(in crate::v02::resource::recovery) use internal::next_transfer_nonce;
pub(in crate::v02::resource::recovery) use internal::publish_attempt;
pub(in crate::v02::resource::recovery) use internal::store_usage;
pub(in crate::v02::resource::recovery) use internal::wait_attempt_async;
pub use recovery_attempt::RecoveryAttempt;
pub use recovery_handle::RecoveryHandle;
pub use recovery_store::RecoveryStore;
pub use recovery_store_shutdown_error::RecoveryStoreShutdownError;
pub use recovery_store_shutdown_handle::RecoveryStoreShutdownHandle;

#[cfg(test)]
use crate::v02::report::RecoveryPhase;
#[cfg(test)]
use crate::v02::report::RecoveryStoreOptions;

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Wake;
    use std::task::Waker;
    use std::time::Duration;

    use super::RecoveryAction;
    use super::RecoveryCore;
    use super::RecoveryPhase;
    use super::RecoveryStoreOptions;
    use super::lock;
    use crate::v02::report::CleanupFailure;
    use crate::v02::report::CleanupStage;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::ManagerId;
    use crate::v02::report::ResourceId;

    struct Noop;
    impl RecoveryAction for Noop {
        fn retry(
            &self,
            _payload: Arc<dyn std::any::Any + Send + Sync>,
        ) -> Pin<Box<dyn Future<Output = Result<(), CleanupFailure>> + Send + 'static>> {
            Box::pin(std::future::ready(Ok(())))
        }
    }

    struct FailOnce {
        calls: AtomicUsize,
        resource_id: ResourceId,
    }
    impl RecoveryAction for FailOnce {
        fn retry(
            &self,
            _payload: Arc<dyn std::any::Any + Send + Sync>,
        ) -> Pin<Box<dyn Future<Output = Result<(), CleanupFailure>> + Send + 'static>> {
            let fail = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
            Box::pin(std::future::ready(if fail {
                Err(CleanupFailure {
                    resource_id: self.resource_id,
                    stage: CleanupStage::Drop,
                    summary: DiagnosticSummary::new("temporary failure"),
                    source: None,
                })
            } else {
                Ok(())
            }))
        }
    }

    struct Probe(Arc<AtomicUsize>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Gate {
        state: Mutex<(bool, Option<Waker>)>,
        started: std::sync::mpsc::Sender<()>,
    }
    impl Gate {
        fn open(&self) {
            let waker = {
                let mut state = lock(&self.state);
                state.0 = true;
                state.1.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
        async fn wait(&self) {
            std::future::poll_fn(|cx| {
                let mut state = lock(&self.state);
                if state.0 {
                    Poll::Ready(())
                } else {
                    state.1 = Some(cx.waker().clone());
                    Poll::Pending
                }
            })
            .await
        }
    }

    struct FlakyCleanup {
        calls: AtomicUsize,
        gate: Arc<Gate>,
    }

    struct PanicCleanup {
        calls: AtomicUsize,
        poll: bool,
    }
    impl super::super::cleanup::AsyncCleanup<Probe> for PanicCleanup {
        fn cleanup(
            self: Arc<Self>,
            resource: Probe,
            _context: crate::v02::context::CleanupContext,
        ) -> super::super::cleanup::CleanupFuture<Probe> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Box::pin(async move { ErrCleanup::failed(resource) });
            }
            if self.poll {
                Box::pin(async move {
                    let _resource = resource;
                    panic!("cleanup poll panic");
                })
            } else {
                panic!("cleanup construction panic");
            }
        }
    }

    fn panic_cleanup_group(poll: bool, store: bool) -> (Arc<RecoveryCore>, Option<super::RecoveryStore>) {
        use crate::v02::manager::capacity::ManagerCapacityPools;
        use crate::v02::manager::capacity::ScopeCapacity;
        use crate::v02::report::ManagerLimits;
        use crate::v02::report::ScopeLimits;

        let manager = ManagerId::new_unique();
        let resource_id = ResourceId::new(manager, 10, 1);
        let pools = ManagerCapacityPools::new(ManagerLimits::default());
        let registration = ScopeCapacity::root(&pools, 83, ScopeLimits::default())
            .resource_registration()
            .unwrap();
        let policy: Arc<dyn super::super::cleanup::AsyncCleanup<Probe>> = Arc::new(PanicCleanup {
            calls: AtomicUsize::new(0),
            poll,
        });
        let (payload, action) =
            super::async_owner(resource_id, Probe(Arc::new(AtomicUsize::new(0))), policy, registration);
        let group = Arc::new(RecoveryCore::new(vec![resource_id], payload, action));
        let recovery_store =
            store.then(|| super::RecoveryStore::new(crate::v02::report::RecoveryStoreOptions::default()).unwrap());
        if let Some(recovery_store) = &recovery_store
            && let Err((_error, _group)) = recovery_store.accept(Arc::clone(&group))
        {
            panic!("store rejected recovery group");
        }
        (group, recovery_store)
    }
    impl super::super::cleanup::AsyncCleanup<Probe> for FlakyCleanup {
        fn cleanup(
            self: Arc<Self>,
            resource: Probe,
            _context: crate::v02::context::CleanupContext,
        ) -> super::super::cleanup::CleanupFuture<Probe> {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            let gate = Arc::clone(&self.gate);
            Box::pin(async move {
                if attempt == 0 {
                    ErrCleanup::failed(resource)
                } else {
                    let _ = gate.started.send(());
                    gate.wait().await;
                    drop(resource);
                    super::super::cleanup::CleanupOutcome::Released
                }
            })
        }
    }

    #[test]
    fn test_cancelled_retry_observer_keeps_real_owner_and_policy_on_store_worker() {
        use crate::v02::manager::capacity::ManagerCapacityPools;
        use crate::v02::manager::capacity::ScopeCapacity;
        use crate::v02::report::ManagerLimits;
        use crate::v02::report::ScopeLimits;

        let manager = ManagerId::new_unique();
        let resource_id = ResourceId::new(manager, 9, 1);
        let drops = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let gate = Arc::new(Gate {
            state: Mutex::new((false, None)),
            started: started_tx,
        });
        let policy = Arc::new(FlakyCleanup {
            calls: AtomicUsize::new(0),
            gate: Arc::clone(&gate),
        });
        let policy_owner = Arc::clone(&policy);
        let policy: Arc<dyn super::super::cleanup::AsyncCleanup<Probe>> = policy;
        let pools = ManagerCapacityPools::new(ManagerLimits::default());
        let registration = ScopeCapacity::root(&pools, 81, ScopeLimits::default())
            .resource_registration()
            .unwrap();
        let (payload, action) = super::async_owner(resource_id, Probe(Arc::clone(&drops)), policy, registration);
        let group = Arc::new(RecoveryCore::new(vec![resource_id], payload, action));
        let handle = super::RecoveryHandle::new(Arc::clone(&group));
        let store = super::RecoveryStore::new(super::RecoveryStoreOptions::default()).unwrap();
        handle.transfer_to(&store).unwrap();

        let first = handle.try_retry().unwrap().wait().unwrap();
        assert_eq!(first.recovery.phase, RecoveryPhase::Pending);
        assert_eq!(handle.snapshot().attempts, 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        let attempt = handle.try_retry().unwrap();
        let mut observer = Box::pin(attempt.wait_async());
        let mut cx = Context::from_waker(Waker::noop());
        assert!(observer.as_mut().poll(&mut cx).is_pending());
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("store worker starts retry");
        drop(observer);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        gate.open();

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while group.snapshot().phase != RecoveryPhase::Resolved {
            assert!(std::time::Instant::now() < deadline, "store retry did not finish");
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(policy_owner.calls.load(Ordering::SeqCst), 2);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(store.request_shutdown().wait_for(Duration::from_secs(2)).unwrap());
    }

    #[test]
    fn test_cancelled_direct_retry_keeps_real_resource_and_capacity_registration() {
        use crate::v02::manager::capacity::ManagerCapacityPools;
        use crate::v02::manager::capacity::ScopeCapacity;
        use crate::v02::report::ManagerLimits;
        use crate::v02::report::ScopeLimits;

        let resource_id = ResourceId::new(ManagerId::new_unique(), 13, 1);
        let drops = Arc::new(AtomicUsize::new(0));
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let gate = Arc::new(Gate {
            state: Mutex::new((false, None)),
            started: started_tx,
        });
        let policy = Arc::new(FlakyCleanup {
            calls: AtomicUsize::new(0),
            gate: Arc::clone(&gate),
        });
        let policy_owner = Arc::clone(&policy);
        let policy: Arc<dyn super::super::cleanup::AsyncCleanup<Probe>> = policy;
        let pools = ManagerCapacityPools::new(ManagerLimits::default());
        let scope_capacity = ScopeCapacity::root(
            &pools,
            84,
            ScopeLimits {
                resources: 1,
                ..ScopeLimits::default()
            },
        );
        let registration = scope_capacity.resource_registration().unwrap();
        let (payload, action) = super::async_owner(resource_id, Probe(Arc::clone(&drops)), policy, registration);
        let group = Arc::new(RecoveryCore::new(vec![resource_id], payload, action));
        let handle = super::RecoveryHandle::new(Arc::clone(&group));

        assert_eq!(
            handle.try_retry().unwrap().wait().unwrap().recovery.phase,
            RecoveryPhase::Pending
        );
        let attempt = handle.try_retry().unwrap();
        let driver_attempt = attempt.as_async();
        let observer_attempt = attempt.as_async();
        let mut driver = Box::pin(driver_attempt.wait());
        let mut observer = Box::pin(observer_attempt.wait());
        let driver_waker = Waker::noop();
        let observer_waker = Waker::noop();
        let mut driver_context = Context::from_waker(driver_waker);
        let mut observer_context = Context::from_waker(observer_waker);

        assert!(driver.as_mut().poll(&mut driver_context).is_pending());
        started_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("direct retry starts cleanup");
        assert!(observer.as_mut().poll(&mut observer_context).is_pending());
        assert!(scope_capacity.resource_registration().is_err());
        drop(driver);

        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let report = match observer.as_mut().poll(&mut observer_context) {
            Poll::Ready(report) => report,
            Poll::Pending => panic!("observer did not receive direct cancellation report"),
        };
        assert_eq!(report.recovery.phase, RecoveryPhase::Unconfirmed);
        assert_eq!(report.recovery.resources.as_ref(), [resource_id]);
        assert!(report.failures.iter().any(|failure| {
            failure.resource_id == resource_id
                && failure.summary.as_str() == "recovery attempt execution cancelled; cleanup unconfirmed"
        }));
        assert!(scope_capacity.resource_registration().is_err());
        assert_eq!(policy_owner.calls.load(Ordering::SeqCst), 2);
        assert!(handle.try_retry().is_err());
        drop(group);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }

    struct ErrCleanup;
    impl ErrCleanup {
        fn failed(resource: Probe) -> super::super::cleanup::CleanupOutcome<Probe> {
            super::super::cleanup::CleanupOutcome::Failed {
                resource,
                failure: CleanupFailure {
                    resource_id: ResourceId::new(ManagerId::new_unique(), 1, 1),
                    stage: CleanupStage::AsyncRelease,
                    summary: DiagnosticSummary::new("retry me"),
                    source: None,
                },
            }
        }
    }

    fn group() -> Arc<RecoveryCore> {
        Arc::new(RecoveryCore::new(
            vec![ResourceId::new(ManagerId::new_unique(), 1, 1)],
            Arc::new(()),
            Arc::new(Noop),
        ))
    }

    #[test]
    fn test_public_store_transfer_retains_group_and_returns_signed_receipt() {
        let store = super::RecoveryStore::new(super::RecoveryStoreOptions::default()).unwrap();
        let handle = super::RecoveryHandle::new(group());
        let same_group = handle.clone();
        let different_group = super::RecoveryHandle::new(group());
        assert_eq!(handle, same_group);
        assert_ne!(handle, different_group);
        assert!(format!("{handle:?}").starts_with("RecoveryHandle("));
        let id = handle.id();
        let transfer = handle.transfer_to(&store).unwrap();
        assert_eq!(transfer.recovery, id);
        assert_eq!(transfer.receipt.recovery(), id);
        assert!(store.contains(id));
        assert_eq!(handle.snapshot().phase, crate::v02::report::RecoveryPhase::Transferred);
        assert_eq!(store.snapshot().groups, 1);
        let shutdown = store.request_shutdown();
        assert!(!shutdown.wait_for(Duration::ZERO).unwrap());
        let attempt = store.try_retry(id).unwrap();
        let report = attempt.wait().unwrap();
        assert_eq!(report.recovery.phase, crate::v02::report::RecoveryPhase::Resolved);
        assert!(shutdown.wait_for(Duration::from_secs(2)).unwrap());
        assert!(shutdown.is_complete());
    }

    #[test]
    fn test_async_recovery_attempt_waits_on_shared_retry() {
        let group = group();
        let attempt = group.try_retry().unwrap();
        let async_attempt = attempt.as_async();
        let report = super::block_on(async_attempt.wait());
        assert_eq!(report.recovery.phase, RecoveryPhase::Resolved);
        assert_eq!(group.snapshot().attempts, 1);
        assert_eq!(
            async_attempt.as_sync().wait().unwrap().recovery.phase,
            RecoveryPhase::Resolved
        );
    }

    #[test]
    fn test_finish_does_not_publish_while_an_async_retry_owns_the_run() {
        struct BlockedRetry(Arc<Gate>);

        impl RecoveryAction for BlockedRetry {
            fn retry(&self, _payload: Arc<dyn std::any::Any + Send + Sync>) -> super::RecoveryFuture {
                let gate = Arc::clone(&self.0);
                Box::pin(async move {
                    gate.wait().await;
                    Ok(())
                })
            }
        }

        let (started_tx, _started_rx) = std::sync::mpsc::channel();
        let gate = Arc::new(Gate {
            state: Mutex::new((false, None)),
            started: started_tx,
        });
        let group = Arc::new(RecoveryCore::new(
            vec![ResourceId::new(ManagerId::new_unique(), 1, 1)],
            Arc::new(()),
            Arc::new(BlockedRetry(Arc::clone(&gate))),
        ));
        let attempt = group.try_retry().unwrap();
        let first_async_attempt = attempt.as_async();
        let second_async_attempt = attempt.as_async();
        let mut first_observer = Box::pin(first_async_attempt.wait());
        let mut second_observer = Box::pin(second_async_attempt.wait());
        let mut context = Context::from_waker(Waker::noop());
        assert!(first_observer.as_mut().poll(&mut context).is_pending());
        assert!(second_observer.as_mut().poll(&mut context).is_pending());
        assert_eq!(group.snapshot().phase, RecoveryPhase::Running);
        drop(second_observer);
        assert_eq!(group.snapshot().phase, RecoveryPhase::Running);

        attempt.finish(Ok(()));
        assert!(first_observer.as_mut().poll(&mut context).is_pending());

        gate.open();
        let first_report = match first_observer.as_mut().poll(&mut context) {
            Poll::Ready(report) => report,
            Poll::Pending => panic!("first observer did not receive the completed retry"),
        };
        assert_eq!(first_report.recovery.phase, RecoveryPhase::Resolved);
        assert_eq!(group.snapshot().phase, RecoveryPhase::Resolved);
    }

    #[test]
    fn test_dropping_unpolled_retry_wait_does_not_take_the_run() {
        let group = group();
        let attempt = group.try_retry().unwrap();
        let observer = attempt.as_async();
        let wait = observer.wait();
        drop(wait);

        assert_eq!(group.snapshot().phase, RecoveryPhase::Running);
        assert!(lock(&attempt.run).is_some());
        let report = super::block_on(observer.wait());
        assert_eq!(report.recovery.phase, RecoveryPhase::Resolved);
    }

    #[test]
    fn test_shutdown_handle_async_wait_and_clones_share_completion() {
        let store = super::RecoveryStore::new(super::RecoveryStoreOptions::default()).unwrap();
        let handle = store.request_shutdown();
        let clone = handle.clone();
        assert_eq!(handle.snapshot().groups, 0);
        assert!(super::block_on(handle.wait_async()).is_ok());
        assert!(handle.is_complete());
        assert!(clone.wait().is_ok());
        assert!(clone.is_complete());
    }

    #[test]
    fn test_shutdown_failure_is_shared_with_clones_and_repeated_waits() {
        let store = super::RecoveryStore::new(super::RecoveryStoreOptions::default()).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        store
            .async_workers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                panic!("injected worker panic");
            }));
        let handle = store.request_shutdown();
        let clone = handle.clone();
        let first = {
            let handle = handle.clone();
            std::thread::spawn(move || handle.wait())
        };
        started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (observer_tx, observer_rx) = std::sync::mpsc::channel();
        let second = {
            let handle = clone.clone();
            std::thread::spawn(move || {
                observer_tx.send(()).unwrap();
                handle.wait()
            })
        };
        observer_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        release_tx.send(()).unwrap();
        let expected = Err(crate::v02::resource::recovery::RecoveryStoreShutdownError::WorkerPanicked);
        assert_eq!(first.join().unwrap(), expected);
        assert_eq!(second.join().unwrap(), expected);
        assert_eq!(clone.wait(), expected);
        assert_eq!(
            clone.wait_for(Duration::ZERO),
            Err(crate::v02::resource::recovery::RecoveryStoreShutdownError::WorkerPanicked)
        );
        assert!(handle.is_complete());
    }

    #[test]
    fn test_shutdown_async_wait_reuses_and_unregisters_its_waker() {
        let store = super::RecoveryStore::with_capacity(2, 2).unwrap();
        let group = group();
        let id = group.id();
        assert!(store.accept(group).is_ok());
        let handle = store.request_shutdown();
        let mut wait = Box::pin(handle.wait_async());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);

        assert!(wait.as_mut().poll(&mut context).is_pending());
        assert!(wait.as_mut().poll(&mut context).is_pending());
        assert_eq!(store.shutdown_notify.waiter_count(), 1);
        drop(wait);
        assert_eq!(store.shutdown_notify.waiter_count(), 0);
        let attempt = store.try_retry(id).unwrap();
        attempt.finish(Ok(()));
        assert!(handle.wait().is_ok());
    }

    #[test]
    fn test_shutdown_notifies_when_group_resolves_before_callback_installation() {
        struct WakeCount(AtomicUsize);
        impl Wake for WakeCount {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let store = super::RecoveryStore::with_capacity(2, 2).unwrap();
        let gate = Arc::new((std::sync::Barrier::new(2), std::sync::Barrier::new(2)));
        *lock(&store.callback_install_gate) = Some(Arc::clone(&gate));
        let group = group();
        let id = group.id();
        let accepting = {
            let store = store.clone();
            std::thread::spawn(move || store.accept(group))
        };

        gate.0.wait();
        let handle = store.request_shutdown();
        let mut wait = Box::pin(handle.wait_async());
        let wake_count = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_count));
        let mut context = Context::from_waker(&waker);
        assert!(wait.as_mut().poll(&mut context).is_pending());

        let attempt = store.try_retry(id).unwrap();
        attempt.finish(Ok(()));
        gate.1.wait();
        assert!(accepting.join().unwrap().is_ok());
        assert!(wake_count.0.load(Ordering::SeqCst) > 0);
        assert!(wait.as_mut().poll(&mut context).is_ready());
    }

    #[test]
    fn test_retry_is_single_flight() {
        let group = group();
        let attempt = group.try_retry().unwrap();
        assert!(group.try_retry().is_err());
        attempt.finish(Ok(()));
        assert!(group.try_retry().is_err());
    }

    #[test]
    fn test_old_report_is_immutable() {
        let group = group();
        let old = group.snapshot();
        let attempt = group.try_retry().unwrap();
        attempt.finish(Err(CleanupFailure {
            resource_id: old.resources[0],
            stage: CleanupStage::AsyncRelease,
            summary: DiagnosticSummary::new("temporary failure"),
            source: None,
        }));
        assert_eq!(old.attempts, 0);
        assert_eq!(old.phase, crate::v02::report::RecoveryPhase::Pending);
        assert_eq!(group.snapshot().attempts, 1);
    }

    #[test]
    fn test_unconfirmed_does_not_publish_released() {
        let group = group();
        let attempt = group.try_retry().unwrap();
        drop(attempt);
        assert_eq!(group.snapshot().phase, crate::v02::report::RecoveryPhase::Unconfirmed);
    }

    #[test]
    fn test_cancelled_direct_retry_publishes_unconfirmed_and_retains_future() {
        struct PendingProbe(Arc<AtomicUsize>);

        impl Future for PendingProbe {
            type Output = Result<(), CleanupFailure>;

            fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
                Poll::Pending
            }
        }

        impl Drop for PendingProbe {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        struct PendingRetry(Arc<AtomicUsize>);

        impl RecoveryAction for PendingRetry {
            fn retry(&self, _payload: Arc<dyn std::any::Any + Send + Sync>) -> super::RecoveryFuture {
                Box::pin(PendingProbe(Arc::clone(&self.0)))
            }
        }

        struct CountWake(AtomicUsize);

        impl Wake for CountWake {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let resource_id = ResourceId::new(ManagerId::new_unique(), 12, 1);
        let group = Arc::new(RecoveryCore::new(
            vec![resource_id],
            Arc::new(()),
            Arc::new(PendingRetry(Arc::clone(&drops))),
        ));
        let attempt = group.try_retry().unwrap();
        let driver_attempt = attempt.as_async();
        let observer_attempt = attempt.as_async();
        let mut driver = Box::pin(driver_attempt.wait());
        let mut observer = Box::pin(observer_attempt.wait());
        let driver_wake = Arc::new(CountWake(AtomicUsize::new(0)));
        let observer_wake = Arc::new(CountWake(AtomicUsize::new(0)));
        let driver_waker = Waker::from(Arc::clone(&driver_wake));
        let observer_waker = Waker::from(Arc::clone(&observer_wake));
        let mut driver_context = Context::from_waker(&driver_waker);
        let mut observer_context = Context::from_waker(&observer_waker);

        assert!(driver.as_mut().poll(&mut driver_context).is_pending());
        assert!(observer.as_mut().poll(&mut observer_context).is_pending());
        assert_eq!(group.snapshot().phase, RecoveryPhase::Running);

        drop(driver);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(observer_wake.0.load(Ordering::SeqCst) > 0);
        let report = match observer.as_mut().poll(&mut observer_context) {
            Poll::Ready(report) => report,
            Poll::Pending => panic!("observer did not receive cancellation report"),
        };
        assert_eq!(report.recovery.phase, RecoveryPhase::Unconfirmed);
        assert_eq!(report.recovery.resources.as_ref(), [resource_id]);
        assert_eq!(group.snapshot().phase, RecoveryPhase::Unconfirmed);
        assert!(group.try_retry().is_err());
        drop(group);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }

    struct PanicRetry;
    impl RecoveryAction for PanicRetry {
        fn retry(&self, _payload: Arc<dyn std::any::Any + Send + Sync>) -> super::RecoveryFuture {
            panic!("retry construction panic")
        }
    }

    struct PanicPoll;
    impl RecoveryAction for PanicPoll {
        fn retry(&self, _payload: Arc<dyn std::any::Any + Send + Sync>) -> super::RecoveryFuture {
            Box::pin(std::future::poll_fn(|_| -> Poll<Result<(), CleanupFailure>> {
                panic!("retry poll panic")
            }))
        }
    }

    struct PanicFutureDropProbe(Arc<AtomicUsize>);

    impl Future for PanicFutureDropProbe {
        type Output = Result<(), CleanupFailure>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            panic!("retry poll panic with retained future")
        }
    }

    impl Drop for PanicFutureDropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct PanicPollWithDrop(Arc<AtomicUsize>);

    impl RecoveryAction for PanicPollWithDrop {
        fn retry(&self, _payload: Arc<dyn std::any::Any + Send + Sync>) -> super::RecoveryFuture {
            Box::pin(PanicFutureDropProbe(Arc::clone(&self.0)))
        }
    }

    struct ReadyDropPanic {
        drops: Arc<AtomicUsize>,
        action_drops: Arc<AtomicUsize>,
        result: bool,
    }

    impl Drop for ReadyDropPanic {
        fn drop(&mut self) {
            self.action_drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct ReadyDropPanicFuture {
        drops: Arc<AtomicUsize>,
        result: bool,
    }

    impl Future for ReadyDropPanicFuture {
        type Output = Result<(), CleanupFailure>;

        fn poll(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Self::Output> {
            if self.result {
                Poll::Ready(Ok(()))
            } else {
                Poll::Ready(Err(CleanupFailure {
                    resource_id: ResourceId::new(ManagerId::new_unique(), 13, 1),
                    stage: CleanupStage::AsyncRelease,
                    summary: DiagnosticSummary::new("retry failed"),
                    source: None,
                }))
            }
        }
    }

    impl Drop for ReadyDropPanicFuture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            panic!("ready future drop panic");
        }
    }

    impl RecoveryAction for ReadyDropPanic {
        fn retry(&self, _payload: Arc<dyn std::any::Any + Send + Sync>) -> super::RecoveryFuture {
            Box::pin(ReadyDropPanicFuture {
                drops: Arc::clone(&self.drops),
                result: self.result,
            })
        }
    }

    struct OwnerDrop(Arc<AtomicUsize>);

    impl Drop for OwnerDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_recovery_ready_drop_panic_is_unconfirmed_direct_and_in_store() {
        for result in [true, false] {
            for in_store in [false, true] {
                let drops = Arc::new(AtomicUsize::new(0));
                let owner_drops = Arc::new(AtomicUsize::new(0));
                let action_drops = Arc::new(AtomicUsize::new(0));
                let manager = ManagerId::new_unique();
                let group = Arc::new(RecoveryCore::new(
                    vec![ResourceId::new(manager, 13, 1)],
                    Arc::new(OwnerDrop(Arc::clone(&owner_drops))),
                    Arc::new(ReadyDropPanic {
                        drops: Arc::clone(&drops),
                        action_drops: Arc::clone(&action_drops),
                        result,
                    }),
                ));
                let store = if in_store {
                    let store = super::RecoveryStore::new(RecoveryStoreOptions::default()).unwrap();
                    assert!(store.accept(Arc::clone(&group)).is_ok());
                    Some(store)
                } else {
                    None
                };
                let attempt = if let Some(store) = &store {
                    store.try_retry(group.id()).unwrap()
                } else {
                    group.try_retry().unwrap()
                };
                let second_observer = attempt.as_async();
                let (report_tx, report_rx) = std::sync::mpsc::channel();
                std::thread::spawn(move || report_tx.send(attempt.wait()).unwrap());
                let report = report_rx
                    .recv_timeout(Duration::from_secs(5))
                    .expect("recovery attempt report was not published within watchdog")
                    .unwrap();
                let second_report = super::block_on(second_observer.wait());

                assert_eq!(report.recovery.phase, RecoveryPhase::Unconfirmed);
                assert_eq!(second_report.recovery.phase, RecoveryPhase::Unconfirmed);
                assert_eq!(report.recovery.phase, second_report.recovery.phase);
                assert_eq!(report.recovery.attempts, second_report.recovery.attempts);
                assert_eq!(report.recovery.resources, second_report.recovery.resources);
                assert_eq!(group.snapshot().phase, RecoveryPhase::Unconfirmed);
                assert_eq!(drops.load(Ordering::SeqCst), 1);
                assert_eq!(owner_drops.load(Ordering::SeqCst), 0);
                assert_eq!(action_drops.load(Ordering::SeqCst), 0);
                assert!(group.try_retry().is_err());
                if let Some(store) = store {
                    assert!(store.contains(group.id()));
                    let healthy = Arc::new(RecoveryCore::new(
                        vec![ResourceId::new(ManagerId::new_unique(), 14, 1)],
                        Arc::new(()),
                        Arc::new(Noop),
                    ));
                    let healthy_id = healthy.id();
                    assert!(store.accept(Arc::clone(&healthy)).is_ok());
                    let healthy_report = store.try_retry(healthy_id).unwrap().wait().unwrap();
                    assert_eq!(healthy_report.recovery.phase, RecoveryPhase::Resolved);
                    store.request_shutdown().wait_for(Duration::from_secs(2)).unwrap();
                }
            }
        }
    }

    #[test]
    fn test_retry_construction_panic_publishes_unconfirmed_report() {
        let manager = ManagerId::new_unique();
        let group = Arc::new(RecoveryCore::new(
            vec![ResourceId::new(manager, 7, 1)],
            Arc::new(()),
            Arc::new(PanicRetry),
        ));
        let attempt = group.try_retry().unwrap();
        let report = super::block_on(attempt.wait_async());
        assert_eq!(report.recovery.phase, crate::v02::report::RecoveryPhase::Unconfirmed);
        assert_eq!(group.snapshot().phase, crate::v02::report::RecoveryPhase::Unconfirmed);
        assert!(group.try_retry().is_err());
    }

    #[test]
    fn test_retry_poll_panic_publishes_unconfirmed_report() {
        let manager = ManagerId::new_unique();
        let group = Arc::new(RecoveryCore::new(
            vec![ResourceId::new(manager, 8, 1)],
            Arc::new(()),
            Arc::new(PanicPoll),
        ));
        let attempt = group.try_retry().unwrap();
        let report = super::block_on(attempt.wait_async());
        assert_eq!(report.recovery.phase, crate::v02::report::RecoveryPhase::Unconfirmed);
        assert_eq!(group.snapshot().phase, crate::v02::report::RecoveryPhase::Unconfirmed);
        assert!(group.try_retry().is_err());
    }

    #[test]
    fn test_retry_poll_panic_retains_future_and_records_cleanup_failure() {
        let manager = ManagerId::new_unique();
        let drops = Arc::new(AtomicUsize::new(0));
        let group = Arc::new(RecoveryCore::new(
            vec![ResourceId::new(manager, 10, 1), ResourceId::new(manager, 11, 1)],
            Arc::new(()),
            Arc::new(PanicPollWithDrop(Arc::clone(&drops))),
        ));
        let report = group.try_retry().unwrap().wait().unwrap();

        assert_eq!(report.recovery.phase, RecoveryPhase::Unconfirmed);
        assert_eq!(report.failures.len(), 2);
        assert!(
            report
                .failures
                .iter()
                .all(|failure| failure.stage == CleanupStage::AsyncRelease)
        );
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(group);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn test_store_retry_panic_publishes_unconfirmed_and_keeps_record() {
        let manager = ManagerId::new_unique();
        let group = Arc::new(RecoveryCore::new(
            vec![ResourceId::new(manager, 9, 1)],
            Arc::new(()),
            Arc::new(PanicPoll),
        ));
        let store = super::RecoveryStore::new(crate::v02::report::RecoveryStoreOptions::default()).unwrap();
        let id = group.id();
        if let Err((_error, _group)) = store.accept(Arc::clone(&group)) {
            panic!("store rejected recovery group");
        }
        let report = store.try_retry(id).unwrap().wait().unwrap();
        assert_eq!(report.recovery.phase, crate::v02::report::RecoveryPhase::Unconfirmed);
        assert_eq!(group.snapshot().phase, crate::v02::report::RecoveryPhase::Unconfirmed);
        assert!(store.contains(id));
        assert!(store.try_retry(id).is_err());
        store.request_shutdown().wait_for(Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn test_async_cleanup_panics_are_unconfirmed_direct_and_in_store() {
        for (poll, in_store) in [(false, false), (true, false), (false, true), (true, true)] {
            let (group, store) = panic_cleanup_group(poll, in_store);
            let attempt = if let Some(store) = &store {
                store.try_retry(group.id()).unwrap()
            } else {
                group.try_retry().unwrap()
            };
            let first = attempt.wait().unwrap();
            assert_eq!(first.recovery.phase, RecoveryPhase::Pending);
            let attempt = if let Some(store) = &store {
                store.try_retry(group.id()).unwrap()
            } else {
                group.try_retry().unwrap()
            };
            let report = attempt.wait().unwrap();
            assert_eq!(report.recovery.phase, RecoveryPhase::Unconfirmed);
            assert_eq!(group.snapshot().phase, RecoveryPhase::Unconfirmed);
            assert!(group.try_retry().is_err());
            if let Some(store) = store {
                assert!(store.contains(group.id()));
                store.request_shutdown().wait_for(Duration::from_secs(2)).unwrap();
            }
        }
    }

    #[test]
    fn test_recovery_store_handoff_is_bounded_and_signed() {
        let manager = ManagerId::new_unique();
        let group = Arc::new(RecoveryCore::new(
            vec![ResourceId::new(manager, 1, 1), ResourceId::new(manager, 2, 1)],
            Arc::new(()),
            Arc::new(Noop),
        ));
        let too_small = super::RecoveryStore::with_capacity(1, 1).unwrap();
        assert!(too_small.accept(Arc::clone(&group)).is_err());
        let handle = super::RecoveryHandle::new(Arc::clone(&group));
        assert_eq!(
            handle.transfer_to(&too_small),
            Err(crate::v02::report::RecoveryTransferError::CapacityExceeded)
        );
        assert_eq!(group.snapshot().phase, crate::v02::report::RecoveryPhase::Pending);

        let store = super::RecoveryStore::with_capacity(1, 2).unwrap();
        let transfer = match store.accept(Arc::clone(&group)) {
            Ok(transfer) => transfer,
            Err((error, _)) => panic!("unexpected store rejection: {error:?}"),
        };
        assert_eq!(transfer.recovery, group.id());
        assert_eq!(transfer.receiver, transfer.receipt.receiver());
        assert!(store.contains(group.id()));
        assert_eq!(group.snapshot().phase, crate::v02::report::RecoveryPhase::Transferred);

        let attempt = store.try_retry(group.id()).unwrap();
        assert_eq!(group.snapshot().phase, crate::v02::report::RecoveryPhase::Running);
        attempt.finish(Ok(()));
        assert_eq!(group.snapshot().phase, crate::v02::report::RecoveryPhase::Resolved);
    }

    #[test]
    fn test_recovery_store_ownership_failed_retry_rejects_second_store() {
        let manager = ManagerId::new_unique();
        let group = Arc::new(RecoveryCore::new(
            vec![ResourceId::new(manager, 11, 1)],
            Arc::new(()),
            Arc::new(FailOnce {
                calls: AtomicUsize::new(0),
                resource_id: ResourceId::new(manager, 11, 1),
            }),
        ));
        let handle = super::RecoveryHandle::new(Arc::clone(&group));
        let store_a = super::RecoveryStore::with_capacity(2, 2).unwrap();
        let store_b = super::RecoveryStore::with_capacity(2, 2).unwrap();
        let id = group.id();

        handle.transfer_to(&store_a).unwrap();
        assert_eq!(
            handle.transfer_to(&store_a),
            Err(crate::v02::report::RecoveryTransferError::AlreadyTransferred),
        );
        let failed = store_a.try_retry(id).unwrap().wait().unwrap();
        assert_eq!(failed.recovery.phase, RecoveryPhase::Pending);
        assert_eq!(group.snapshot().phase, RecoveryPhase::Pending);

        assert_eq!(
            handle.transfer_to(&store_b),
            Err(crate::v02::report::RecoveryTransferError::AlreadyTransferred),
        );
        assert!(store_a.contains(id));
        assert!(!store_b.contains(id));

        assert!(!store_a.request_shutdown().wait_for(Duration::ZERO).unwrap());
        assert_eq!(
            handle.try_retry().unwrap().wait().unwrap().recovery.phase,
            RecoveryPhase::Resolved
        );
        assert!(store_a.request_shutdown().wait_for(Duration::from_secs(2)).unwrap());
        assert!(super::block_on(store_b.request_shutdown().wait_async()).is_ok());
    }

    #[test]
    fn test_recovery_store_ownership_rejects_pending_and_unconfirmed_duplicates() {
        let manager = ManagerId::new_unique();
        let store_a = super::RecoveryStore::with_capacity(2, 2).unwrap();
        let store_b = super::RecoveryStore::with_capacity(2, 2).unwrap();
        let pending = Arc::new(RecoveryCore::new(
            vec![ResourceId::new(manager, 12, 1)],
            Arc::new(()),
            Arc::new(Noop),
        ));
        let pending_handle = super::RecoveryHandle::new(Arc::clone(&pending));
        pending_handle.transfer_to(&store_a).unwrap();
        assert_eq!(pending.snapshot().phase, RecoveryPhase::Transferred);
        assert_eq!(
            pending.try_retry().unwrap().wait().unwrap().recovery.phase,
            RecoveryPhase::Resolved,
        );
        assert_eq!(
            pending_handle.transfer_to(&store_b),
            Err(crate::v02::report::RecoveryTransferError::AlreadyTransferred),
        );
        assert!(!store_b.contains(pending.id()));

        let unconfirmed = Arc::new(RecoveryCore::new_unconfirmed(
            vec![ResourceId::new(manager, 13, 1)],
            Arc::new(()),
        ));
        let unconfirmed_handle = super::RecoveryHandle::new(Arc::clone(&unconfirmed));
        unconfirmed_handle.transfer_to(&store_a).unwrap();
        assert_eq!(unconfirmed.snapshot().phase, RecoveryPhase::Transferred);
        assert_eq!(
            unconfirmed_handle.transfer_to(&store_b),
            Err(crate::v02::report::RecoveryTransferError::AlreadyTransferred),
        );
        assert!(!store_b.contains(unconfirmed.id()));
        // The unconfirmed group cannot be retried; observe that its store
        // remains pending without waiting indefinitely for shutdown.
        assert!(!store_a.request_shutdown().wait_for(Duration::ZERO).unwrap());
        assert!(store_b.request_shutdown().wait_for(Duration::from_secs(2)).unwrap());
    }

    #[test]
    fn test_recovery_store_ownership_rejects_second_store_after_retry_becomes_unconfirmed() {
        let (group, store_a) = panic_cleanup_group(false, true);
        let store_a = store_a.unwrap();
        let store_b = super::RecoveryStore::with_capacity(2, 2).unwrap();
        let handle = super::RecoveryHandle::new(Arc::clone(&group));

        assert_eq!(
            store_a.try_retry(group.id()).unwrap().wait().unwrap().recovery.phase,
            RecoveryPhase::Pending
        );
        assert_eq!(
            store_a.try_retry(group.id()).unwrap().wait().unwrap().recovery.phase,
            RecoveryPhase::Unconfirmed,
        );
        assert_eq!(
            handle.transfer_to(&store_b),
            Err(crate::v02::report::RecoveryTransferError::AlreadyTransferred),
        );
        assert!(store_a.contains(group.id()));
        assert!(!store_b.contains(group.id()));
        assert!(!store_a.request_shutdown().wait_for(Duration::ZERO).unwrap());
        assert!(store_b.request_shutdown().wait_for(Duration::from_secs(2)).unwrap());
    }

    #[test]
    fn test_recovery_store_ownership_competing_first_transfers_only_one_succeeds() {
        let manager = ManagerId::new_unique();
        let group = Arc::new(RecoveryCore::new(
            vec![ResourceId::new(manager, 14, 1)],
            Arc::new(()),
            Arc::new(Noop),
        ));
        let stores = [
            super::RecoveryStore::with_capacity(2, 2).unwrap(),
            super::RecoveryStore::with_capacity(2, 2).unwrap(),
        ];
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let threads = stores.clone().map(|store| {
            let group = Arc::clone(&group);
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                store.accept(group)
            })
        });
        barrier.wait();
        let results = threads.map(|thread| thread.join().unwrap());
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
        assert_eq!(stores.iter().filter(|store| store.contains(group.id())).count(), 1);
        let owner = stores.iter().find(|store| store.contains(group.id())).unwrap();
        owner.try_retry(group.id()).unwrap().wait().unwrap();
        for store in &stores {
            assert!(store.request_shutdown().wait_for(Duration::from_secs(2)).unwrap());
        }
    }

    #[test]
    fn test_recovery_store_ownership_initial_rejections_do_not_claim_group() {
        let manager = ManagerId::new_unique();
        let group = Arc::new(RecoveryCore::new(
            vec![ResourceId::new(manager, 15, 1)],
            Arc::new(()),
            Arc::new(Noop),
        ));
        let handle = super::RecoveryHandle::new(Arc::clone(&group));
        let closed = super::RecoveryStore::with_capacity(2, 2).unwrap();
        closed.close_admission();
        assert_eq!(
            handle.transfer_to(&closed),
            Err(crate::v02::report::RecoveryTransferError::Closed)
        );

        let full = super::RecoveryStore::with_capacity(1, 2).unwrap();
        let occupant = Arc::new(RecoveryCore::new(
            vec![ResourceId::new(manager, 16, 1)],
            Arc::new(()),
            Arc::new(Noop),
        ));
        assert!(full.accept(Arc::clone(&occupant)).is_ok());
        assert_eq!(
            handle.transfer_to(&full),
            Err(crate::v02::report::RecoveryTransferError::CapacityExceeded),
        );
        assert_eq!(group.snapshot().phase, RecoveryPhase::Pending);

        let invalid = Arc::new(RecoveryCore::new(Vec::new(), Arc::new(()), Arc::new(Noop)));
        let invalid_handle = super::RecoveryHandle::new(Arc::clone(&invalid));
        let accepting = super::RecoveryStore::with_capacity(2, 2).unwrap();
        assert_eq!(
            invalid_handle.transfer_to(&accepting),
            Err(crate::v02::report::RecoveryTransferError::InvalidGroup),
        );
        assert_eq!(invalid.snapshot().phase, RecoveryPhase::Pending);

        handle.transfer_to(&accepting).unwrap();
        assert_eq!(
            accepting.try_retry(group.id()).unwrap().wait().unwrap().recovery.phase,
            RecoveryPhase::Resolved
        );
        assert_eq!(
            full.try_retry(occupant.id()).unwrap().wait().unwrap().recovery.phase,
            RecoveryPhase::Resolved
        );
        for store in [&closed, &full, &accepting] {
            assert!(store.request_shutdown().wait_for(Duration::from_secs(2)).unwrap());
        }
    }
}

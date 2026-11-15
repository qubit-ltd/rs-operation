// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Strongly retained recovery payloads and single-flight retry state.

mod async_recovery_attempt;
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
    use std::task::Waker;
    use std::time::Duration;

    use super::RecoveryAction;
    use super::RecoveryCore;
    use super::RecoveryPhase;
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
}

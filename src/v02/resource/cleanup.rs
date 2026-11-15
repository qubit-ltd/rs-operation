// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Bounded, per-scope handoff queue for resource cleanup.
#![allow(dead_code)] // The manager and scope assembly is completed by T11.

mod async_cleanup;
mod async_cleanup_completion;
mod async_cleanup_job;
mod cleanup_dispatcher;
mod cleanup_dispatcher_error;
mod cleanup_outcome;
mod cleanup_pump;
mod cleanup_transfer_error;
mod cleanup_transfer_receiver;
mod default_drop_job;
mod internal;
mod unconfirmed_cleanup;

use std::sync::Mutex;

pub use async_cleanup::AsyncCleanup;
pub use async_cleanup::CleanupFuture;
pub(crate) use async_cleanup_completion::AsyncCleanupCompletion;
pub(crate) use async_cleanup_job::AsyncCleanupJob;
pub(crate) use cleanup_dispatcher::CleanupDispatcher;
pub(crate) use cleanup_dispatcher_error::CleanupDispatcherError;
pub use cleanup_outcome::CleanupOutcome;
pub(crate) use cleanup_pump::CleanupPump;
#[cfg(test)]
pub(crate) use cleanup_transfer_error::CleanupTransferError;
#[cfg(test)]
pub(crate) use cleanup_transfer_receiver::CleanupTransferReceiver;
#[cfg(test)]
pub(crate) use default_drop_job::DefaultDropJob;
pub(crate) use unconfirmed_cleanup::UnconfirmedCleanup;

#[cfg(test)]
use super::cleanup_port::CleanupDispatch;
#[cfg(test)]
use super::cleanup_port::CleanupJob;

/// Locks cleanup state while recovering from a poisoned mutex.
///
/// # Type Parameters
///
/// * `T` — type of the state protected by the mutex.
///
/// # Parameters
///
/// - `mutex`: Shared state mutex to lock.
///
/// # Returns
///
/// The mutex guard, including when a prior panic poisoned the mutex.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc;
    use std::time::Duration;

    use super::AsyncCleanup;
    use super::AsyncCleanupJob;
    use super::CleanupDispatch;
    use super::CleanupDispatcher;
    use super::CleanupJob;
    use super::CleanupOutcome;
    use super::CleanupTransferError;
    use super::CleanupTransferReceiver;
    use super::DefaultDropJob;
    use crate::v02::executor::CleanupExecutor;
    use crate::v02::report::CleanupFailure;
    use crate::v02::report::CleanupStage;
    use crate::v02::report::ContextOptions;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::ManagerId;
    use crate::v02::report::PortableExecutorOptions;
    use crate::v02::report::ResourceId;
    use crate::v02::report::ResourceScopeId;
    use crate::v02::report::ThreadPoolExecutorOptions;

    fn executor() -> Arc<CleanupExecutor> {
        Arc::new(
            CleanupExecutor::new(
                ThreadPoolExecutorOptions {
                    worker_count: 1,
                    queue_capacity: 1,
                },
                PortableExecutorOptions {
                    worker_count: 1,
                    task_capacity: 2,
                    polls_per_turn: 2,
                },
            )
            .unwrap(),
        )
    }

    struct FailCleanup;
    impl AsyncCleanup<usize> for FailCleanup {
        fn cleanup(
            self: Arc<Self>,
            resource: usize,
            _context: crate::v02::context::CleanupContext,
        ) -> super::CleanupFuture<usize> {
            Box::pin(std::future::ready(CleanupOutcome::Failed {
                failure: CleanupFailure {
                    resource_id: ResourceId::new(ManagerId::new_unique(), 1, 1),
                    stage: CleanupStage::AsyncRelease,
                    summary: DiagnosticSummary::new("retry required"),
                    source: None,
                },
                resource,
            }))
        }
    }

    struct ConfirmCleanup;
    impl AsyncCleanup<usize> for ConfirmCleanup {
        fn cleanup(
            self: Arc<Self>,
            _resource: usize,
            _context: crate::v02::context::CleanupContext,
        ) -> super::CleanupFuture<usize> {
            Box::pin(std::future::ready(CleanupOutcome::Released))
        }
    }

    struct PanicCleanup;
    impl AsyncCleanup<usize> for PanicCleanup {
        fn cleanup(
            self: Arc<Self>,
            resource: usize,
            _context: crate::v02::context::CleanupContext,
        ) -> super::CleanupFuture<usize> {
            Box::pin(async move {
                std::hint::black_box(&resource);
                panic!("async cleanup panic");
            })
        }
    }

    struct DropOnSchedule(mpsc::Sender<String>);
    impl Drop for DropOnSchedule {
        fn drop(&mut self) {
            self.0
                .send(std::thread::current().name().unwrap_or("unnamed").into())
                .ok();
        }
    }
    impl CleanupJob for DropOnSchedule {
        fn schedule(self: Box<Self>, executor: Arc<CleanupExecutor>) -> Result<(), Box<dyn CleanupJob>> {
            match executor.reserve_drop() {
                Ok(permit) => {
                    permit.drop_value(self);
                    Ok(())
                }
                Err(_) => Err(self),
            }
        }
    }

    #[test]
    fn test_default_drop_runs_on_cleanup_worker() {
        let dispatcher = CleanupDispatcher::new(1, executor()).unwrap();
        let (drop_send, drop_receive) = mpsc::channel();
        let (complete_send, complete_receive) = mpsc::channel();
        struct Probe(mpsc::Sender<()>);
        impl Drop for Probe {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let job = DefaultDropJob::new(Probe(drop_send), None, move |evidence| {
            let thread = std::thread::current().name().unwrap_or("unnamed").to_owned();
            if let Ok(evidence) = evidence {
                let _ = complete_send.send((evidence, thread));
            }
        });
        dispatcher.reserve().unwrap().enqueue(Box::new(job));
        // Enqueue immediately kicks the dispatcher; the explicit drive sees
        // no remaining pending work.
        assert_eq!(dispatcher.drive(1), 0);
        drop_receive.recv_timeout(Duration::from_secs(1)).unwrap();
        let (evidence, thread_name) = complete_receive.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(evidence, crate::v02::report::CleanupEvidence::DropCompleted);
        assert!(thread_name.starts_with("qubit-operation-"));
    }

    #[test]
    fn test_panicking_default_drop_is_unconfirmed() {
        struct PanicOnDrop;
        impl Drop for PanicOnDrop {
            fn drop(&mut self) {
                panic!("cleanup panic");
            }
        }
        let executor = executor();
        let (send, receive) = mpsc::channel();
        let job = DefaultDropJob::new(PanicOnDrop, None, move |result| {
            let _ = send.send(result);
        });
        assert!(Box::new(job).schedule(Arc::clone(&executor)).is_ok());
        let result = receive.recv_timeout(Duration::from_secs(1)).unwrap();
        let Err(mut unconfirmed) = result else {
            panic!("a panicking destructor cannot publish DropCompleted");
        };
        assert!(unconfirmed.panic_payload().is::<&'static str>());
        assert!(unconfirmed.take_registration().is_none());
    }

    #[test]
    fn test_closed_admission_does_not_discard_reserved_job() {
        let dispatcher = CleanupDispatcher::new(1, executor()).unwrap();
        assert_eq!(dispatcher.capacity(), 1);
        let permit = dispatcher.reserve().unwrap();
        dispatcher.close_admission();
        let (send, receive) = mpsc::channel();
        permit.enqueue(Box::new(DropOnSchedule(send)));
        assert_eq!(dispatcher.drive(1), 0);
        assert!(receive.recv_timeout(Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn test_executor_saturation_keeps_owner_in_pending_queue() {
        struct HoldWorker(mpsc::Receiver<()>);
        impl Drop for HoldWorker {
            fn drop(&mut self) {
                let _ = self.0.recv();
            }
        }
        let executor = executor();
        let (started_send, started_receive) = mpsc::channel();
        let (release_send, release_receive) = mpsc::channel();
        let (release_second, release_second_receive) = mpsc::channel();
        struct SignalThenHold(mpsc::Sender<()>, mpsc::Receiver<()>);
        impl Drop for SignalThenHold {
            fn drop(&mut self) {
                let _ = self.0.send(());
                let _ = self.1.recv();
            }
        }
        executor
            .reserve_drop()
            .unwrap()
            .drop_value(SignalThenHold(started_send, release_receive));
        started_receive.recv_timeout(Duration::from_secs(1)).unwrap();
        executor
            .reserve_drop()
            .unwrap()
            .drop_value(HoldWorker(release_second_receive));

        let dispatcher = CleanupDispatcher::new(1, Arc::clone(&executor)).unwrap();
        let (job_send, job_receive) = mpsc::channel();
        dispatcher
            .reserve()
            .unwrap()
            .enqueue(Box::new(DropOnSchedule(job_send)));
        assert_eq!(dispatcher.drive(1), 0);
        assert_eq!(dispatcher.pending_count(), 1);
        assert_eq!(dispatcher.reserved_count(), 1);
        assert!(job_receive.try_recv().is_err());

        release_send.send(()).unwrap();
        release_second.send(()).unwrap();
        for _ in 0..1_000 {
            if dispatcher.drive(1) == 1 {
                break;
            }
            std::thread::yield_now();
        }
        assert!(job_receive.recv_timeout(Duration::from_secs(1)).is_ok());
        assert_eq!(dispatcher.pending_count(), 0);
        assert_eq!(dispatcher.reserved_count(), 0);
    }

    #[test]
    fn test_async_failure_preserves_t_and_policy() {
        let executor = executor();
        let policy = Arc::new(FailCleanup);
        let policy_erased: Arc<dyn AsyncCleanup<usize>> = Arc::clone(&policy) as Arc<_>;
        let completion = Arc::new(std::sync::Mutex::new(None));
        let job = AsyncCleanupJob::new(
            41usize,
            Arc::clone(&policy_erased),
            crate::v02::context::CleanupContext::new(ContextOptions::default()).unwrap(),
            Arc::clone(&completion),
        );
        assert!(Box::new(job).schedule(Arc::clone(&executor)).is_ok());
        assert_eq!(executor.poll_async_turn(), 1);

        let completion = completion.lock().unwrap().take().unwrap();
        assert!(Arc::ptr_eq(&completion.policy, &policy_erased));
        assert_eq!(completion.evidence, None);
        match completion.outcome.unwrap() {
            CleanupOutcome::Failed { resource, .. } => assert_eq!(resource, 41),
            _ => panic!("failed cleanup must retain the resource"),
        }
    }

    #[test]
    fn test_async_executor_saturation_keeps_resource_and_policy_queued() {
        let executor = executor();
        executor.reserve_async().unwrap().spawn(async {});
        executor.reserve_async().unwrap().spawn(async {});
        let policy: Arc<dyn AsyncCleanup<usize>> = Arc::new(FailCleanup);
        let completion = Arc::new(std::sync::Mutex::new(None));
        let dispatcher = CleanupDispatcher::new(1, Arc::clone(&executor)).unwrap();
        let job = AsyncCleanupJob::new(
            73,
            Arc::clone(&policy),
            crate::v02::context::CleanupContext::new(ContextOptions::default()).unwrap(),
            Arc::clone(&completion),
        );
        dispatcher.reserve().unwrap().enqueue(Box::new(job));
        assert_eq!(dispatcher.drive(1), 0);
        assert_eq!(dispatcher.pending_count(), 1);
        assert_eq!(dispatcher.reserved_count(), 1);
        executor.poll_async_turn();
        assert_eq!(dispatcher.drive(1), 1);
        executor.poll_async_turn();
        assert_eq!(dispatcher.pending_count(), 0);
        let result = completion.lock().unwrap().take().unwrap();
        assert!(Arc::ptr_eq(&result.policy, &policy));
        match result.outcome.unwrap() {
            CleanupOutcome::Failed { resource, .. } => assert_eq!(resource, 73),
            _ => panic!("failed cleanup must retain its resource"),
        }
    }

    #[test]
    fn test_async_success_reports_confirmation() {
        let executor = executor();
        let policy: Arc<dyn AsyncCleanup<usize>> = Arc::new(ConfirmCleanup);
        let completion = Arc::new(std::sync::Mutex::new(None));
        let job = AsyncCleanupJob::new(
            19,
            Arc::clone(&policy),
            crate::v02::context::CleanupContext::new(ContextOptions::default()).unwrap(),
            Arc::clone(&completion),
        );
        assert!(Box::new(job).schedule(Arc::clone(&executor)).is_ok());
        executor.poll_async_turn();
        let completion = completion.lock().unwrap().take().unwrap();
        assert!(matches!(completion.outcome, Some(CleanupOutcome::Released)));
        assert_eq!(
            completion.evidence,
            Some(crate::v02::report::CleanupEvidence::Confirmed)
        );
    }

    #[test]
    fn test_cleanup_transfer_requires_receiver_acceptance() {
        let manager = ManagerId::new_unique();
        let source_scope = ResourceScopeId::new_unique();
        let receiver_scope = ResourceScopeId::new_unique();
        let first_receiver = CleanupTransferReceiver::new(manager, receiver_scope, 1, executor()).unwrap();
        let first_id = ResourceId::new(manager, 1, 1);
        assert!(
            first_receiver
                .accept(first_id, source_scope, Box::new(DropOnSchedule(mpsc::channel().0)),)
                .is_ok()
        );

        let (drop_send, drop_receive) = mpsc::channel();
        struct Probe(mpsc::Sender<()>);
        impl Drop for Probe {
            fn drop(&mut self) {
                let _ = self.0.send(());
            }
        }
        let second_id = ResourceId::new(manager, 2, 1);
        let job: Box<dyn CleanupJob> = Box::new(DefaultDropJob::new(Probe(drop_send), None, |_| {}));
        let (error, returned_job) = match first_receiver.accept(second_id, source_scope, job) {
            Ok(_) => panic!("a full receiver must not sign a transfer"),
            Err(failure) => failure,
        };
        assert_eq!(error, CleanupTransferError::Full);
        assert!(drop_receive.try_recv().is_err());

        let second_receiver =
            CleanupTransferReceiver::new(manager, ResourceScopeId::new_unique(), 1, executor()).unwrap();
        let receipt = match second_receiver.accept(second_id, source_scope, returned_job) {
            Ok(receipt) => receipt,
            Err(_) => panic!("open receiver with capacity must accept owner"),
        };
        assert_eq!(receipt.resource(), second_id);
        assert!(second_receiver.contains(second_id));
        assert_eq!(second_receiver.drive(1), 1);
        drop_receive.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(first_receiver.drive(1), 1);

        second_receiver.close_admission();
        let (error, _job) = match second_receiver.accept(
            ResourceId::new(manager, 3, 1),
            source_scope,
            Box::new(DropOnSchedule(mpsc::channel().0)),
        ) {
            Ok(_) => panic!("a closed receiver must reject new cleanup transfers"),
            Err(failure) => failure,
        };
        assert_eq!(error, CleanupTransferError::Closed);
    }

    #[test]
    fn test_cleanup_transfer_callback_runs_after_acceptance_outside_queue_lock() {
        let manager = ManagerId::new_unique();
        let source_scope = ResourceScopeId::new_unique();
        let receiver_scope = ResourceScopeId::new_unique();
        let holder = Arc::new(std::sync::Mutex::new(None::<Arc<CleanupTransferReceiver>>));
        let callback_holder = Arc::clone(&holder);
        let callbacks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let callback_count = Arc::clone(&callbacks);
        let receiver = Arc::new(
            CleanupTransferReceiver::new_with_receipt_callback(
                manager,
                receiver_scope,
                1,
                executor(),
                move |receipt| {
                    let receiver = callback_holder.lock().unwrap().as_ref().unwrap().clone();
                    assert!(receiver.contains(receipt.resource()));
                    callback_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                },
            )
            .unwrap(),
        );
        *holder.lock().unwrap() = Some(Arc::clone(&receiver));
        let resource = ResourceId::new(manager, 19, 1);
        let receipt = match receiver.accept(resource, source_scope, Box::new(DropOnSchedule(mpsc::channel().0))) {
            Ok(receipt) => receipt,
            Err(_) => panic!("open receiver should accept transfer"),
        };
        assert_eq!(receipt.resource(), resource);
        assert_eq!(callbacks.load(std::sync::atomic::Ordering::SeqCst), 1);

        let second = ResourceId::new(manager, 20, 1);
        let job: Box<dyn CleanupJob> = Box::new(DropOnSchedule(mpsc::channel().0));
        let (error, _job) = receiver.accept(second, source_scope, job).unwrap_err();
        assert_eq!(error, CleanupTransferError::Full);
        assert_eq!(callbacks.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn test_cleanup_transfer_callback_panic_does_not_revoke_acceptance() {
        let manager = ManagerId::new_unique();
        let source_scope = ResourceScopeId::new_unique();
        let receiver = CleanupTransferReceiver::new_with_receipt_callback(
            manager,
            ResourceScopeId::new_unique(),
            1,
            executor(),
            |_| panic!("observer callback panic"),
        )
        .unwrap();
        let resource = ResourceId::new(manager, 21, 1);
        let receipt = match receiver.accept(resource, source_scope, Box::new(DropOnSchedule(mpsc::channel().0))) {
            Ok(receipt) => receipt,
            Err(_) => panic!("open receiver should accept transfer"),
        };
        assert_eq!(receipt.resource(), resource);
        assert!(receiver.contains(resource));
    }

    #[test]
    fn test_unconfirmed_async_panic_keeps_owner_and_has_no_evidence() {
        let executor = executor();
        let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let resource = ProbeResource(Arc::clone(&dropped));
        let policy: Arc<dyn AsyncCleanup<ProbeResource>> = Arc::new(PanicProbeCleanup);
        let completion = Arc::new(std::sync::Mutex::new(None));
        let job = AsyncCleanupJob::new(
            resource,
            Arc::clone(&policy),
            crate::v02::context::CleanupContext::new(ContextOptions::default()).unwrap(),
            Arc::clone(&completion),
        );
        assert!(Box::new(job).schedule(Arc::clone(&executor)).is_ok());
        assert_eq!(executor.poll_async_turn(), 1);
        assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 0);
        let completion = completion.lock().unwrap().take().unwrap();
        assert!(completion.outcome.is_none());
        assert!(completion.evidence.is_none());
        assert!(completion.unconfirmed.is_some());
        assert!(Arc::ptr_eq(&completion.policy, &policy));
    }

    #[test]
    fn test_async_cleanup_panic_returns_unconfirmed_completion() {
        let executor = executor();
        let policy: Arc<dyn AsyncCleanup<usize>> = Arc::new(PanicCleanup);
        let completion = Arc::new(std::sync::Mutex::new(None));
        let job = AsyncCleanupJob::new(
            41,
            Arc::clone(&policy),
            crate::v02::context::CleanupContext::new(ContextOptions::default()).unwrap(),
            Arc::clone(&completion),
        );

        assert!(Box::new(job).schedule(Arc::clone(&executor)).is_ok());
        assert_eq!(executor.poll_async_turn(), 1);

        let completion = completion.lock().unwrap().take().unwrap();
        assert!(completion.outcome.is_none());
        assert!(completion.evidence.is_none());
        assert!(completion.unconfirmed.is_some());
        assert!(Arc::ptr_eq(&completion.policy, &policy));
    }

    struct PanicProbeCleanup;
    impl AsyncCleanup<ProbeResource> for PanicProbeCleanup {
        fn cleanup(
            self: Arc<Self>,
            resource: ProbeResource,
            _context: crate::v02::context::CleanupContext,
        ) -> super::CleanupFuture<ProbeResource> {
            Box::pin(PanicFuture(Some(resource)))
        }
    }

    struct PanicFuture(Option<ProbeResource>);
    impl std::future::Future for PanicFuture {
        type Output = CleanupOutcome<ProbeResource>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _context: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            std::hint::black_box(&self.0);
            panic!("async cleanup panic");
        }
    }

    struct ProbeResource(Arc<std::sync::atomic::AtomicUsize>);
    impl Drop for ProbeResource {
        fn drop(&mut self) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

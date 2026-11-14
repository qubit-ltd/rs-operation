use super::{
    ManagerCore, OperationJob, OperationManager, SubmissionLease, SubmissionPermit, SubmissionPort,
    SubmissionReservation,
};
use crate::v02::operation::OutcomeDelivery;
use crate::v02::report::{AdmissionError, OperationOptions};
use crate::v02::report::{ContextOptions, ScopeLimits};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};

#[test]
fn typed_sync_submission_publishes_outcome_and_settlement() {
    let (_, root) = ManagerCore::for_test();
    let mut handle = root
        .try_submit(crate::v02::report::OperationOptions::default(), |_| {
            Ok::<_, ()>(42usize)
        })
        .unwrap();
    assert!(matches!(
        handle.try_take_outcome().unwrap(),
        Some(crate::v02::report::OperationOutcome::Succeeded(42))
    ));
    assert!(handle.wait_settlement().unwrap().all_released());
}

#[test]
fn typed_async_submission_preserves_target_and_publishes_lifecycle() {
    let (_, root) = ManagerCore::for_test();
    let mut handle = root
        .as_async()
        .try_submit(OperationOptions::default(), |_| async {
            Ok::<_, ()>(24usize)
        })
        .unwrap()
        .into_sync();
    assert!(matches!(
        handle.try_take_outcome().unwrap(),
        Some(crate::v02::report::OperationOutcome::Succeeded(24))
    ));
    assert!(handle.wait_settlement().unwrap().all_released());
}

struct SingleWorkerPort(mpsc::Sender<OperationJob>, Arc<LeaseCounts>);
struct SingleWorkerPermit(mpsc::Sender<OperationJob>);
#[derive(Default)]
struct LeaseCounts {
    execution: AtomicUsize,
    settlement: AtomicUsize,
    delivered: AtomicUsize,
    discarded: AtomicUsize,
}
struct CountingLease(Arc<LeaseCounts>);

impl SubmissionPort for SingleWorkerPort {
    fn reserve(
        &self,
        _parent: Option<&super::ScopeCore>,
        _options: &OperationOptions,
    ) -> Result<SubmissionReservation, AdmissionError> {
        Ok(SubmissionReservation {
            execution: Box::new(SingleWorkerPermit(self.0.clone())),
            lifecycle: Arc::new(CountingLease(Arc::clone(&self.1))),
        })
    }
}

impl SubmissionLease for CountingLease {
    fn execution_finished(&self) {
        self.0.execution.fetch_add(1, Ordering::SeqCst);
    }
    fn settlement_finished(&self) {
        self.0.settlement.fetch_add(1, Ordering::SeqCst);
    }
    fn outcome_delivered(&self, delivery: OutcomeDelivery) {
        match delivery {
            OutcomeDelivery::Delivered => &self.0.delivered,
            OutcomeDelivery::Discarded => &self.0.discarded,
        }
        .fetch_add(1, Ordering::SeqCst);
    }
}

impl SubmissionPermit for SingleWorkerPermit {
    fn dispatch(self: Box<Self>, job: OperationJob) {
        self.0.send(job).expect("single worker remains alive");
    }
}

#[test]
fn typed_sync_parent_settles_after_child_on_one_worker() {
    let (sender, receiver) = mpsc::channel::<OperationJob>();
    let worker = std::thread::spawn(move || {
        while let Ok(job) = receiver.recv() {
            match job {
                OperationJob::Sync(job) => job(),
                OperationJob::Async(mut future) => {
                    let waker = std::task::Waker::noop();
                    let mut context = std::task::Context::from_waker(waker);
                    while future.as_mut().poll(&mut context).is_pending() {
                        std::thread::yield_now();
                    }
                }
            }
        }
    });
    let counts = Arc::new(LeaseCounts::default());
    let port = Arc::new(SingleWorkerPort(sender, Arc::clone(&counts)));
    let core = ManagerCore::new(
        port,
        crate::v02::context::OperationContext::new(ContextOptions::default()).unwrap(),
        ScopeLimits::default(),
    );
    let root = core.root_view();
    let mut parent = root
        .try_submit(OperationOptions::default(), |scope| {
            let child = scope
                .manager()
                .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
                .unwrap();
            drop(child.into_observer());
            Ok::<_, ()>(17usize)
        })
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        crate::v02::report::OperationOutcome::Succeeded(17)
    ));
    let report = parent.wait_settlement().unwrap();
    assert_eq!(report.children_total, 1);
    assert_eq!(counts.execution.load(Ordering::SeqCst), 2);
    assert_eq!(counts.settlement.load(Ordering::SeqCst), 2);
    assert_eq!(counts.delivered.load(Ordering::SeqCst), 1);
    assert_eq!(counts.discarded.load(Ordering::SeqCst), 1);
    drop(parent);
    drop(root);
    drop(core);
    worker.join().unwrap();
}

#[test]
fn typed_async_parent_settles_after_child_on_one_worker() {
    let (sender, receiver) = mpsc::channel::<OperationJob>();
    let worker = std::thread::spawn(move || {
        while let Ok(job) = receiver.recv() {
            match job {
                OperationJob::Sync(job) => job(),
                OperationJob::Async(mut future) => {
                    let waker = std::task::Waker::noop();
                    let mut context = std::task::Context::from_waker(waker);
                    while future.as_mut().poll(&mut context).is_pending() {
                        std::thread::yield_now();
                    }
                }
            }
        }
    });
    let port = Arc::new(SingleWorkerPort(sender, Arc::new(LeaseCounts::default())));
    let core = ManagerCore::new(
        port,
        crate::v02::context::OperationContext::new(ContextOptions::default()).unwrap(),
        ScopeLimits::default(),
    );
    let root = core.root_view();
    let mut parent = root
        .as_async()
        .try_submit(OperationOptions::default(), |scope| async move {
            let child = scope
                .manager()
                .as_async()
                .try_submit(OperationOptions::default(), |_| async { Ok::<_, ()>(()) })
                .unwrap();
            drop(child.into_observer());
            Ok::<_, ()>(31usize)
        })
        .unwrap()
        .into_sync();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        crate::v02::report::OperationOutcome::Succeeded(31)
    ));
    assert_eq!(parent.wait_settlement().unwrap().children_total, 1);
    drop(parent);
    drop(root);
    drop(core);
    worker.join().unwrap();
}

#[test]
fn required_child_failure_changes_parent_outcome_but_not_settlement_fact() {
    let (sender, receiver) = mpsc::channel::<OperationJob>();
    let worker = std::thread::spawn(move || {
        while let Ok(job) = receiver.recv() {
            match job {
                OperationJob::Sync(job) => job(),
                OperationJob::Async(mut future) => {
                    let waker = std::task::Waker::noop();
                    let mut context = std::task::Context::from_waker(waker);
                    while future.as_mut().poll(&mut context).is_pending() {
                        std::thread::yield_now();
                    }
                }
            }
        }
    });
    let port = Arc::new(SingleWorkerPort(sender, Arc::new(LeaseCounts::default())));
    let core = ManagerCore::new(
        port,
        crate::v02::context::OperationContext::new(ContextOptions::default()).unwrap(),
        ScopeLimits::default(),
    );
    let root = core.root_view();
    let mut parent = root
        .try_submit(OperationOptions::default(), |scope| {
            let child = scope
                .manager()
                .try_submit(OperationOptions::default(), |_| Err::<(), _>("failed"))
                .unwrap();
            drop(child.into_observer());
            Ok::<_, ()>(99usize)
        })
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        crate::v02::report::OperationOutcome::ChildFailed(_)
    ));
    let settlement = parent.wait_settlement().unwrap();
    assert_eq!(
        settlement.status,
        crate::v02::report::SettlementStatus::Released
    );
    assert_eq!(settlement.children_total, 1);
    drop(parent);
    drop(root);
    drop(core);
    worker.join().unwrap();
}

struct DropCounter(Arc<AtomicUsize>);
impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn rejected_typed_submission_retains_original_body() {
    let (core, root) = ManagerCore::for_test();
    super::lock(&core.state).phase = super::ManagerPhase::Closed;
    let drops = Arc::new(AtomicUsize::new(0));
    let marker = DropCounter(Arc::clone(&drops));
    let error = match root.try_submit(OperationOptions::default(), move |_| {
        let _marker = marker;
        Ok::<_, ()>(())
    }) {
        Ok(_) => panic!("closed manager accepted submission"),
        Err(error) => error,
    };
    assert_eq!(error.reason(), AdmissionError::Closed);
    let (_, body) = error.into_parts();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(body);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

fn submit_work(manager: &OperationManager, value: usize) -> Result<(), AdmissionError> {
    manager
        .try_submit_body(move |scope| {
            scope
                .manager()
                .try_submit_body(move |grandchild| {
                    grandchild.record_test_value(value);
                })
                .unwrap();
        })
        .map(|_| ())
        .map_err(|error| error.into_parts().0)
}

#[test]
fn same_helper_works_at_every_depth() {
    let (core, root) = ManagerCore::for_test();
    submit_work(&root, 7).unwrap();
    let relationships = core.test_relationships();
    assert_eq!(relationships.len(), 2);
    assert_eq!(relationships[0].1, None);
    assert_eq!(relationships[1].1, Some(relationships[0].0));
    assert_eq!(core.test_values(), vec![7]);
}

#[test]
fn manager_conversion_preserves_parent() {
    let (core, root) = ManagerCore::for_test();
    root.try_submit_body(|scope| {
        let parent = scope.id();
        scope
            .manager()
            .as_async()
            .as_sync()
            .try_submit_body(|_| {})
            .unwrap();
        assert_eq!(parent, scope.id());
    })
    .unwrap();
    let relationships = core.test_relationships();
    assert_eq!(relationships.len(), 2);
    assert_eq!(relationships[1].1, Some(relationships[0].0));
}

#[test]
fn root_manager_inside_operation_stays_root() {
    let (core, root) = ManagerCore::for_test();
    let root_clone = root.clone();
    root.try_submit_body(move |_| {
        root_clone.try_submit_body(|_| {}).unwrap();
    })
    .unwrap();
    let relationships = core.test_relationships();
    assert_eq!(relationships.len(), 2);
    assert_eq!(relationships[1].1, None);
}

#[test]
fn expired_parent_does_not_fall_back_to_root() {
    let saved = Arc::new(Mutex::new(None));
    let saved_in_body = Arc::clone(&saved);
    let (_, root) = ManagerCore::for_test();
    root.try_submit_body(move |scope| {
        *saved_in_body.lock().unwrap() = Some(scope.manager());
    })
    .unwrap();
    let manager = saved.lock().unwrap().take().unwrap();
    assert_eq!(
        manager.try_submit_body(|_| {}).unwrap_err().reason(),
        AdmissionError::ScopeEnded
    );
}

#[test]
fn frame_end_closes_scope_even_when_scope_moves_out() {
    let escaped = Arc::new(Mutex::new(None));
    let escaped_in_body = Arc::clone(&escaped);
    let (_, root) = ManagerCore::for_test();
    root.try_submit_body(move |scope| {
        *escaped_in_body.lock().unwrap() = Some(scope);
    })
    .unwrap();
    let scope = escaped.lock().unwrap().take().unwrap();
    assert!(!scope.is_open());
    assert_eq!(
        scope
            .manager()
            .try_submit_body(|_| {})
            .unwrap_err()
            .reason(),
        AdmissionError::ScopeEnded
    );
}

#[test]
fn async_manager_conversion_keeps_parent_and_runs_grandchildren() {
    let (core, root) = ManagerCore::for_test();
    root.try_submit_body(|scope| {
        let async_manager = scope.manager().as_async();
        async_manager
            .try_submit_body(|child_scope| async move {
                child_scope.manager().try_submit_body(|_| {}).unwrap();
            })
            .unwrap();
    })
    .unwrap();

    let relationships = core.test_relationships();
    assert_eq!(relationships.len(), 3);
    assert_eq!(relationships[0].1, None);
    assert_eq!(relationships[1].1, Some(relationships[0].0));
    assert_eq!(relationships[2].1, Some(relationships[1].0));
}

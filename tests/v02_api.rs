use qubit_operation::{
    ManagerOptions, OperationManager, OperationOptions, OperationOutcome, ShutdownMode,
};

#[path = "v02_api_negative/mod.rs"]
mod api_negative;

struct OpaqueValue(std::sync::Mutex<usize>);
struct OpaqueError;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    struct ThreadWake(std::thread::Thread);
    impl std::task::Wake for ThreadWake {
        fn wake(self: std::sync::Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = std::task::Waker::from(std::sync::Arc::new(ThreadWake(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(output) => return output,
            std::task::Poll::Pending => std::thread::park(),
        }
    }
}

#[test]
fn sync_and_async_views_share_one_result_delivery() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let _async_manager = manager.as_async();
    let handle = manager
        .try_submit(OperationOptions::default(), |_| {
            Ok::<_, OpaqueError>(OpaqueValue(std::sync::Mutex::new(17)))
        })
        .unwrap();
    let async_handle = handle.into_async();
    let mut sync_handle = async_handle.into_sync();
    let outcome = sync_handle.wait_outcome().unwrap();
    match outcome {
        OperationOutcome::Succeeded(value) => {
            assert_eq!(*value.0.lock().unwrap(), 17);
        }
        _ => panic!("expected the single successful outcome"),
    }
    assert!(sync_handle.wait_settlement().unwrap().all_released());
    manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .unwrap();
}

#[test]
fn root_and_scope_manager_run_the_same_submission_function() {
    fn submit(manager: &OperationManager) -> qubit_operation::OperationHandle<usize, ()> {
        manager
            .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(41))
            .unwrap()
    }
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let mut parent = manager
        .try_submit(OperationOptions::default(), move |scope| {
            let mut child = submit(&scope.manager());
            match child.wait_outcome().unwrap() {
                OperationOutcome::Succeeded(n) => Ok::<_, ()>(n + 1),
                _ => Err(()),
            }
        })
        .unwrap();
    assert!(matches!(
        parent.wait_outcome().unwrap(),
        OperationOutcome::Succeeded(42)
    ));
    parent.wait_settlement().unwrap();
    manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .unwrap();
}

#[test]
fn async_manager_runs_a_non_clone_non_debug_result_and_settles_it() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let async_manager = manager.as_async();
    let mut operation = async_manager
        .try_submit(OperationOptions::default(), |_| async {
            Ok::<_, OpaqueError>(OpaqueValue(std::sync::Mutex::new(29)))
        })
        .unwrap();
    match block_on(operation.wait_outcome()).unwrap() {
        OperationOutcome::Succeeded(value) => assert_eq!(*value.0.lock().unwrap(), 29),
        _ => panic!("async operation did not produce its single success outcome"),
    }
    assert!(block_on(operation.wait_settlement()).all_released());
    manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .unwrap();
}

#[test]
fn converting_to_observer_preserves_execution_but_dropping_handle_requests_stop() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handle = manager
        .try_submit(OperationOptions::default(), move |_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let observer = handle.observer();
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    let observer_only = handle.into_observer();
    assert!(observer.snapshot().stop.is_none());
    release_tx.send(()).unwrap();
    observer_only.wait_settlement().unwrap();
    assert!(observer.snapshot().stop.is_none());

    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let handle = manager
        .try_submit(OperationOptions::default(), move |_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let observer = handle.observer();
    entered_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    drop(handle);
    assert_eq!(
        observer.snapshot().stop.unwrap().reason,
        qubit_operation::StopReason::UserCancelled
    );
    release_tx.send(()).unwrap();
    observer.wait_settlement().unwrap();
    manager
        .request_shutdown(ShutdownMode::Drain)
        .wait()
        .unwrap();
}

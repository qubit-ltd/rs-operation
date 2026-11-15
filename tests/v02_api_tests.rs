// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::env::temp_dir;
use std::env::var_os;
use std::fs;
use std::future::Future;
use std::path::PathBuf;
use std::process::Command;
use std::process::id;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::task::Context as TaskContext;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;
use std::thread::Thread;
use std::thread::current;
use std::thread::park;
use std::time::Duration;

use qubit_operation::ManagerOptions;
use qubit_operation::OperationHandle;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::ShutdownMode;
use qubit_operation::StopReason;

static NEXT_CASE: AtomicUsize = AtomicUsize::new(0);

struct OpaqueValue(Mutex<usize>);
struct OpaqueError;

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(Thread);
    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(ThreadWake(current())));
    let mut context = TaskContext::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => park(),
        }
    }
}

#[test]
fn test_sync_and_async_views_share_one_result_delivery() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let _async_manager = manager.as_async();
    let handle = manager
        .try_submit(OperationOptions::default(), |_| {
            Ok::<_, OpaqueError>(OpaqueValue(Mutex::new(17)))
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
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(shutdown_report.snapshot.active_operations, 0);
}

#[test]
fn test_root_and_scope_manager_run_the_same_submission_function() {
    fn submit(manager: &OperationManager) -> OperationHandle<usize, ()> {
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
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(shutdown_report.snapshot.active_operations, 0);
}

#[test]
fn test_async_manager_runs_a_non_clone_non_debug_result_and_settles_it() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let async_manager = manager.as_async();
    let mut operation = async_manager
        .try_submit(OperationOptions::default(), |_| async {
            Ok::<_, OpaqueError>(OpaqueValue(Mutex::new(29)))
        })
        .unwrap();
    match block_on(operation.wait_outcome()).unwrap() {
        OperationOutcome::Succeeded(value) => assert_eq!(*value.0.lock().unwrap(), 29),
        _ => panic!("async operation did not produce its single success outcome"),
    }
    assert!(block_on(operation.wait_settlement()).all_released());
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(shutdown_report.snapshot.active_operations, 0);
}

#[test]
fn test_public_api_rejects_the_removed_or_forbidden_capabilities() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures = manifest.join("tests/v02_api_negative");
    let deps = std::env::current_exe().unwrap().parent().unwrap().to_path_buf();
    let library = fs::read_dir(&deps)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("libqubit_operation-") && name.ends_with(".rlib"))
        })
        .expect("cargo test built qubit_operation rlib");
    let rustc = var_os("RUSTC").unwrap_or_else(|| "rustc".into());

    for entry in fs::read_dir(fixtures).unwrap().filter_map(Result::ok) {
        let source = entry.path();
        if source.extension().and_then(|ext| ext.to_str()) != Some("rs") {
            continue;
        }
        let contents = fs::read_to_string(&source).unwrap();
        let expected = contents
            .lines()
            .find_map(|line| line.strip_prefix("// EXPECT: "))
            .unwrap_or_else(|| panic!("{} has no exact expected diagnostic", source.display()));
        let serial = NEXT_CASE.fetch_add(1, Ordering::Relaxed);
        let output = temp_dir().join(format!("qubit_operation_negative_{}_{}.rlib", id(), serial));
        let result = Command::new(&rustc)
            .arg("--edition=2024")
            .arg("--crate-type=lib")
            .arg("--crate-name=public_api_negative")
            .arg("--extern")
            .arg(format!("qubit_operation={}", library.display()))
            .arg("-L")
            .arg(format!("dependency={}", deps.display()))
            .arg(&source)
            .arg("-o")
            .arg(&output)
            .output()
            .expect("run rustc for API contract fixture");
        let stderr = String::from_utf8_lossy(&result.stderr);
        assert!(!result.status.success(), "{} unexpectedly compiled", source.display());
        assert!(
            stderr.contains(expected),
            "{} failed for an unrelated reason; expected diagnostic fragment {expected:?}, got:\n{stderr}",
            source.display()
        );
        let _ = fs::remove_file(output);
    }
}

#[test]
fn test_converting_to_observer_preserves_execution_but_dropping_handle_requests_stop() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handle = manager
        .try_submit(OperationOptions::default(), move |_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let observer = handle.observer();
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let observer_only = handle.into_observer();
    assert!(observer.snapshot().stop.is_none());
    release_tx.send(()).unwrap();
    observer_only.wait_settlement().unwrap();
    assert!(observer.snapshot().stop.is_none());

    let (entered_tx, entered_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let handle = manager
        .try_submit(OperationOptions::default(), move |_| {
            entered_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            Ok::<_, ()>(())
        })
        .unwrap();
    let observer = handle.observer();
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(handle);
    assert_eq!(observer.snapshot().stop.unwrap().reason, StopReason::UserCancelled);
    release_tx.send(()).unwrap();
    observer.wait_settlement().unwrap();
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait().unwrap();
    assert_eq!(shutdown_report.snapshot.active_operations, 0);
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::mpsc;
use std::time::Duration;

use qubit_operation::GroupOptions;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::ServiceResourceOptions;
use qubit_operation::ShutdownMode;
use qubit_operation::ShutdownWait;
use qubit_operation::ThreadPoolExecutorOptions;

struct DropNotice(Option<mpsc::Sender<()>>);

impl Drop for DropNotice {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            sender.send(()).unwrap();
        }
    }
}

#[test]
fn test_service_refs_delay_close() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let service = group.resource_scope(ServiceResourceOptions { capacity: 1 }).unwrap();
    assert_eq!(group.snapshot().service_resource_scopes, 1);
    let (drop_tx, drop_rx) = mpsc::channel();
    let value = service.manage(DropNotice(Some(drop_tx))).unwrap();
    let key = value.key();
    drop(value);
    let held = service.get_resource(key).unwrap();
    let shutdown = manager.request_shutdown(ShutdownMode::Drain);
    assert_eq!(shutdown.snapshot().service_resources, 1);
    assert!(drop_rx.try_recv().is_err());
    assert!(matches!(
        shutdown.wait_for(Duration::ZERO).unwrap(),
        ShutdownWait::Pending(_)
    ));
    drop(held);
    drop_rx.recv().expect("last service reference enables cleanup");
    assert!(matches!(
        shutdown.wait_for(Duration::from_secs(2)).unwrap(),
        ShutdownWait::Completed(_)
    ));
    assert_eq!(group.snapshot().service_resource_scopes, 0);
}

#[test]
fn test_bounded_cleanup_queue_is_consumed_after_worker_capacity_returns() {
    /// Coordinates when cleanup drops may continue on the worker thread.
    struct Gate {
        open: Mutex<bool>,
        changed: Condvar,
    }

    impl Gate {
        /// Blocks the cleanup worker until the test opens the gate.
        fn wait(&self) {
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.changed.wait(open).unwrap();
            }
        }

        /// Allows the blocked cleanup worker to continue.
        fn open(&self) {
            *self.open.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }

    /// Announces its cleanup start and holds the worker until the gate opens.
    struct Probe {
        id: usize,
        starts: mpsc::Sender<usize>,
        gate: Arc<Gate>,
    }

    impl Drop for Probe {
        fn drop(&mut self) {
            let _ = self.starts.send(self.id);
            self.gate.wait();
        }
    }

    let options = ManagerOptions {
        cleanup_executor: ThreadPoolExecutorOptions {
            worker_count: 1,
            queue_capacity: 1,
        },
        ..ManagerOptions::default()
    };
    let manager = OperationManager::new(options).unwrap();
    let group = manager.group(GroupOptions { capacity: 1 }).unwrap();
    let service = group.resource_scope(ServiceResourceOptions { capacity: 3 }).unwrap();
    let (starts_tx, starts_rx) = mpsc::channel();
    let gate = Arc::new(Gate {
        open: Mutex::new(false),
        changed: Condvar::new(),
    });
    let mut refs = Vec::new();
    for id in 0..3 {
        let sender = starts_tx.clone();
        refs.push(
            service
                .manage(Probe {
                    id,
                    starts: sender,
                    gate: Arc::clone(&gate),
                })
                .unwrap(),
        );
    }
    service.close();
    drop(refs);
    let running = starts_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("cleanup worker starts first job");
    gate.open();
    let mut observed = vec![running];
    while observed.len() < 3 {
        observed.push(
            starts_rx
                .recv_timeout(Duration::from_secs(2))
                .expect("pump retries pending cleanup after permit return"),
        );
    }
    observed.sort_unstable();
    assert_eq!(observed, vec![0, 1, 2]);
    let shutdown = manager.request_shutdown(ShutdownMode::Drain);
    assert!(matches!(
        shutdown.wait_for(Duration::from_secs(2)).unwrap(),
        ShutdownWait::Completed(_)
    ));
}

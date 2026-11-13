// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for crate-private execution and result lease contracts.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use crate::report::AdmissionError;
use crate::report::SupervisorLimits;
use crate::supervisor::OperationSupervisor;

fn limits(active_roots: usize, queued_roots: usize, unobserved_results: usize) -> SupervisorLimits {
    SupervisorLimits {
        active_roots,
        queued_roots,
        unobserved_results,
        admission_waiters: 1,
        ..SupervisorLimits::default()
    }
}

#[test]
fn test_result_slot_limit_is_one_shot_and_released_separately() {
    let supervisor = OperationSupervisor::new(limits(1, 0, 1));
    let mut first = supervisor.try_admit_body(String::from("body")).unwrap();
    let work = first.transfer().unwrap();
    drop(first);
    let (body, task_lease, result_lease) = work.into_parts();
    assert_eq!(body, "body");
    assert_eq!(supervisor.active_roots(), 1);
    assert_eq!(supervisor.reserved_results(), 1);
    assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

    task_lease.complete();
    assert_eq!(supervisor.active_roots(), 0);
    assert_eq!(supervisor.reserved_results(), 1);
    assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);

    result_lease.observe();
    assert_eq!(supervisor.reserved_results(), 0);
    assert!(supervisor.try_admit().is_ok());
}

#[test]
fn test_dropping_admission_handle_does_not_release_transferred_running_body_quota() {
    let supervisor = OperationSupervisor::new(SupervisorLimits {
        active_roots: 1,
        queued_roots: 0,
        unobserved_results: 1,
        ..SupervisorLimits::default()
    });
    let runs = Arc::new(AtomicUsize::new(0));
    let body_runs = Arc::clone(&runs);
    let mut admission = supervisor
        .try_admit_body(move || body_runs.fetch_add(1, Ordering::SeqCst))
        .unwrap();
    let work = admission.transfer().unwrap();
    let (body, execution, result) = work.into_parts();
    drop(admission);

    assert_eq!(body(), 0);
    assert_eq!(supervisor.active_roots(), 1);
    assert_eq!(supervisor.reserved_results(), 1);
    assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    execution.complete();
    assert_eq!(supervisor.active_roots(), 0);
    assert_eq!(supervisor.reserved_results(), 1);
    assert_eq!(supervisor.try_admit().unwrap_err(), AdmissionError::Busy);
    result.observe();
    assert_eq!(supervisor.reserved_results(), 0);
}

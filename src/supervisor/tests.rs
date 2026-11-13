// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests that inspect the supervisor's private waiter registration state.

use core::future::Future;
use core::task::Context;
use core::task::Poll;

use super::OperationSupervisor;
use super::internal::util::lock;
use crate::report::AdmissionError;
use crate::report::SupervisorLimits;

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
fn test_waiters_are_bounded_cancel_safely_and_recover_rejected_input() {
    let supervisor = OperationSupervisor::new(limits(0, 1, 2));
    let accepted = supervisor.try_admit().unwrap();
    let mut waiting = Box::pin(supervisor.start_wait_body(String::from("kept")).unwrap());
    let mut context = Context::from_waker(std::task::Waker::noop());
    assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
    let rejected = supervisor.start_wait_body(String::from("rejected")).unwrap_err();
    assert_eq!(rejected.reason(), AdmissionError::Busy);
    assert_eq!(rejected.into_body(), "rejected");
    drop(waiting);
    drop(accepted);
    assert_eq!(lock(&supervisor.inner.state).waiters.len(), 0);
}

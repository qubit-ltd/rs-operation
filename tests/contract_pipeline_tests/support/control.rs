// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::report::ChildBusinessOutcome;
use qubit_operation::report::ChildBusinessSummary;
use qubit_operation::report::ChildSettlement;
use qubit_operation::report::ChildSnapshot;
use qubit_operation::report::DiagnosticSummary;
use qubit_operation::report::OperationId;
use qubit_operation::report::OperationPhase;
use qubit_operation::report::ResourcePhase;
use qubit_operation::report::StopReason;

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

/// Business fact that can be delivered independently from resource release.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BusinessResult {
    /// The operation completed its business work successfully.
    Succeeded,
    /// The operation completed its business work with an error.
    Failed,
}

#[derive(Default)]
struct ControlState {
    business: Option<BusinessResult>,
    released: bool,
    stop_requests: usize,
    first_stop: Option<StopReason>,
    business_waiters: Vec<Waker>,
    release_waiters: Vec<Waker>,
}

/// In-memory operation control driven only by explicit test actions.
#[derive(Clone)]
pub struct FakeControl {
    operation_id: OperationId,
    state: Arc<Mutex<ControlState>>,
}

impl FakeControl {
    /// Create a fresh control with distinct operation identity and no completed
    /// facts.
    pub fn new() -> Self {
        Self {
            operation_id: OperationId::new(NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed) as u128),
            state: Arc::default(),
        }
    }

    /// Return the identity assigned when this control was created.
    pub fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Return the number of actual calls to
    /// [`ManagedOperationControl::request_stop`].
    pub fn stop_requests(&self) -> usize {
        self.state.lock().expect("control mutex poisoned").stop_requests
    }

    /// Return the first accepted stop reason without discarding later call
    /// counts.
    pub fn first_stop_reason(&self) -> Option<StopReason> {
        self.state.lock().expect("control mutex poisoned").first_stop
    }

    /// Publish the business fact and wake observers that polled before it was
    /// available.
    pub fn complete_business(&self, result: BusinessResult) {
        let waiters = {
            let mut state = self.state.lock().expect("control mutex poisoned");
            assert!(state.business.is_none(), "business fact set twice");
            state.business = Some(result);
            std::mem::take(&mut state.business_waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    /// Confirm release independently from the business outcome and wake its
    /// observers.
    pub fn confirm_release(&self) {
        let waiters = {
            let mut state = self.state.lock().expect("control mutex poisoned");
            assert!(!state.released, "release confirmed twice");
            state.released = true;
            std::mem::take(&mut state.release_waiters)
        };
        for waiter in waiters {
            waiter.wake();
        }
    }

    fn register_waiter(waiters: &mut Vec<Waker>, waker: &Waker) {
        if !waiters.iter().any(|registered| registered.will_wake(waker)) {
            waiters.push(waker.clone());
        }
    }
}

impl ManagedOperationControl for FakeControl {
    fn request_stop(&self, reason: StopReason) {
        let mut state = self.state.lock().expect("control mutex poisoned");
        state.stop_requests += 1;
        state.first_stop.get_or_insert(reason);
    }

    fn snapshot(&self) -> ChildSnapshot {
        let state = self.state.lock().expect("control mutex poisoned");
        let phase = if state.business.is_some() {
            OperationPhase::ResultReady
        } else {
            OperationPhase::Running
        };
        let resource_phase = if state.released {
            ResourcePhase::Released
        } else if state.stop_requests > 0 {
            ResourcePhase::Stopping
        } else {
            ResourcePhase::Active
        };
        ChildSnapshot {
            operation_id: self.operation_id,
            phase,
            resource_phase,
            stop_requested: state.stop_requests > 0,
            released: state.released,
        }
    }

    fn poll_business(&self, context: &mut Context<'_>) -> Poll<ChildBusinessSummary> {
        let mut state = self.state.lock().expect("control mutex poisoned");
        match state.business {
            Some(result) => Poll::Ready(ChildBusinessSummary {
                operation_id: self.operation_id,
                outcome: match result {
                    BusinessResult::Succeeded => ChildBusinessOutcome::Succeeded,
                    BusinessResult::Failed => ChildBusinessOutcome::Failed,
                },
                summary: DiagnosticSummary::new(match result {
                    BusinessResult::Succeeded => "succeeded",
                    BusinessResult::Failed => "failed",
                }),
            }),
            None => {
                Self::register_waiter(&mut state.business_waiters, context.waker());
                Poll::Pending
            }
        }
    }

    fn poll_released(&self, context: &mut Context<'_>) -> Poll<ChildSettlement> {
        let mut state = self.state.lock().expect("control mutex poisoned");
        if state.released {
            Poll::Ready(ChildSettlement {
                operation_id: self.operation_id,
                released: true,
                summary: DiagnosticSummary::new("released"),
            })
        } else {
            Self::register_waiter(&mut state.release_waiters, context.waker());
            Poll::Pending
        }
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Driver and shared state for externally executed operations.

use std::future::Future;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;

use super::ChildOutcomeSummary;
use super::DiagnosticSummary;
use super::ExternalExecutionContext;
use super::ManagedOperationControl;
use super::OperationOutcome;
use super::OperationScope;
use super::child_outcome_summary;
use super::lock;
use super::panic_message;

/// Shared one-shot callback for publishing a typed external operation outcome.
pub type ExternalOutcomePublisher<T, E> = Arc<Mutex<Option<Box<dyn FnOnce(OperationOutcome<T, E>) + Send + 'static>>>>;

/// Shared result of claiming an external task for its operation identity.
pub type ExternalBindResult = Arc<Mutex<Option<Result<(), crate::v02::report::ExecutionFailure>>>>;

/// Drives an external task until its outcome and required cleanup are observed.
///
/// # Type Parameters
///
/// - `T`: Successful value produced by the external task.
/// - `E`: Error value produced by the external task.
/// - `P`: Prepared external task implementation.
///
/// # Parameters
///
/// - `scope`: Manager-owned scope assigned to the task.
/// - `adapter`: Prepared task being activated and polled.
/// - `bind_state`: Shared result of the task's identity claim.
/// - `managed_state`: Shared manager control installed before dispatch.
/// - `execution_state`: Shared execution context used to finalize the task.
/// - `publisher_state`: One-shot callback that receives the task outcome.
/// - `activation_permitted`: Whether manager lifecycle state permits starting.
pub async fn drive_external<T, E, P>(
    scope: OperationScope,
    adapter: Arc<P>,
    bind_state: ExternalBindResult,
    managed_state: Arc<Mutex<Option<Arc<ManagedOperationControl>>>>,
    execution_state: Arc<Mutex<Option<Arc<ExternalExecutionContext>>>>,
    publisher_state: ExternalOutcomePublisher<T, E>,
    activation_permitted: bool,
) where
    T: Send + 'static,
    E: Send + 'static,
    P: crate::v02::adapter::PreparedExternalOperation<T, E>,
{
    let operation_id = scope.id();
    let scope_core_for_settlement = Arc::clone(&scope.core);
    let context = scope.context();
    let bind = lock(&bind_state)
        .take()
        .expect("accepted external operation has a bind result");
    let mut activated = false;
    let mut typed = if let Err(failure) = bind {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| adapter.clone().abandon()));
        Some(OperationOutcome::ExecutionFailed(failure))
    } else if !activation_permitted {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| adapter.clone().abandon()));
        Some(OperationOutcome::Stopped(crate::v02::report::StopReport {
            reason: context
                .stop_reason()
                .unwrap_or(crate::v02::report::StopReason::ParentStopped),
            accepted_at: None,
        }))
    } else {
        match panic::catch_unwind(AssertUnwindSafe(|| adapter.snapshot())) {
            Err(payload) => {
                let _ = panic::catch_unwind(AssertUnwindSafe(|| adapter.clone().abandon()));
                Some(OperationOutcome::ExecutionFailed(
                    crate::v02::report::ExecutionFailure {
                        operation_id,
                        summary: DiagnosticSummary::new(&panic_message(payload.as_ref())),
                        source: None,
                    },
                ))
            }
            Ok(snapshot) if snapshot.operation_id != operation_id => {
                let _ = panic::catch_unwind(AssertUnwindSafe(|| adapter.clone().abandon()));
                Some(OperationOutcome::ExecutionFailed(
                    crate::v02::report::ExecutionFailure {
                        operation_id,
                        summary: DiagnosticSummary::new("external snapshot identity mismatch"),
                        source: None,
                    },
                ))
            }
            Ok(_) => match panic::catch_unwind(AssertUnwindSafe(|| adapter.clone().activate(scope))) {
                Ok(Ok(())) => {
                    activated = true;
                    None
                }
                Ok(Err(failure)) => Some(OperationOutcome::ExecutionFailed(failure)),
                Err(payload) => Some(OperationOutcome::ExecutionFailed(
                    crate::v02::report::ExecutionFailure {
                        operation_id,
                        summary: DiagnosticSummary::new(&panic_message(payload.as_ref())),
                        source: None,
                    },
                )),
            },
        }
    };
    let managed = lock(&managed_state)
        .as_ref()
        .cloned()
        .expect("manager control is installed before dispatch");
    let finish_execution = || {
        if let Some(context) = lock(&execution_state).as_ref() {
            context.finish();
        }
    };
    let mut published = false;
    let mut summary: Option<ChildOutcomeSummary> = None;
    let mut release = false;
    let mut release_polled = false;
    let mut settlement: Option<Arc<crate::v02::report::SettlementReport>> = None;
    let mut typed_poll_done = typed.is_some();
    let mut stop_signal = Box::pin(context.stop_requested());
    let mut stop_forwarded = false;
    let mut summary_poll_done = !activated;
    let mut summary_poll_failed = false;
    let mut typed_poll_failed = false;
    let mut release_poll_failed = false;
    let mut settlement_poll_failed = false;
    if activated && let Some(reason) = context.stop_reason() {
        let _ = panic::catch_unwind(AssertUnwindSafe(|| adapter.request_stop(reason)));
        stop_forwarded = true;
    }
    if typed_poll_done {
        if let Some(callback) = lock(&publisher_state).take() {
            callback(typed.take().expect("outcome exists"));
            published = true;
            finish_execution();
        }
    }
    std::future::poll_fn(|cx| {
        if activated && !stop_forwarded {
            if let Poll::Ready(reason) = stop_signal.as_mut().poll(cx) {
                let _ = panic::catch_unwind(AssertUnwindSafe(|| adapter.request_stop(reason)));
                stop_forwarded = true;
            }
        }
        if activated && !typed_poll_done {
            match panic::catch_unwind(AssertUnwindSafe(|| adapter.poll_take_outcome(cx))) {
                Ok(Poll::Ready(outcome)) => {
                    typed = Some(outcome);
                    typed_poll_done = true;
                }
                Ok(Poll::Pending) => {}
                Err(payload) => {
                    typed = Some(OperationOutcome::ExecutionFailed(
                        crate::v02::report::ExecutionFailure {
                            operation_id,
                            summary: DiagnosticSummary::new(&panic_message(payload.as_ref())),
                            source: None,
                        },
                    ));
                    typed_poll_done = true;
                    typed_poll_failed = true;
                }
            }
        }
        if activated && !summary_poll_done {
            match panic::catch_unwind(AssertUnwindSafe(|| adapter.poll_outcome(cx))) {
                Ok(Poll::Ready(value)) => {
                    summary = Some(value);
                    summary_poll_done = true;
                }
                Ok(Poll::Pending) => {}
                Err(_) => {
                    summary_poll_done = true;
                    summary_poll_failed = true;
                }
            }
        }
        if !release_polled && !release_poll_failed {
            match panic::catch_unwind(AssertUnwindSafe(|| adapter.poll_release(cx))) {
                Ok(Poll::Ready(value)) => {
                    release_polled = true;
                    release = managed.confirm_input_release(value);
                }
                Ok(Poll::Pending) => {}
                Err(_) => release_poll_failed = true,
            }
        }
        if settlement.is_none() && !settlement_poll_failed {
            match panic::catch_unwind(AssertUnwindSafe(|| adapter.poll_settlement(cx))) {
                Ok(Poll::Ready(value)) if value.operation_id == operation_id => settlement = Some(value),
                Ok(Poll::Ready(_)) => settlement_poll_failed = true,
                Ok(Poll::Pending) => {}
                Err(_) => settlement_poll_failed = true,
            }
        }
        if typed_poll_done && !published && (!activated || summary_poll_done || typed_poll_failed) {
            if activated {
                let expected = typed.as_ref().map(|value| child_outcome_summary(operation_id, value));
                if summary_poll_failed
                    || summary
                        .as_ref()
                        .is_none_or(|value| Some(value) != expected.as_ref() || value.operation_id != operation_id)
                {
                    typed = Some(OperationOutcome::ExecutionFailed(
                        crate::v02::report::ExecutionFailure {
                            operation_id,
                            summary: DiagnosticSummary::new("external outcome summary mismatch"),
                            source: None,
                        },
                    ));
                }
            }
            if let Some(callback) = lock(&publisher_state).take() {
                if let Some(outcome) = typed.take() {
                    callback(outcome);
                    finish_execution();
                    published = true;
                }
            }
        }
        if published && release && settlement.is_some() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
    scope_core_for_settlement.finish_external_driver_settlement();
}

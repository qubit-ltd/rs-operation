// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use crate::v02::report::RecoverySnapshot;
use crate::v02::resource::recovery::lock;

/// Report and waiter state shared by synchronous and asynchronous observers.
///
/// The report is published once a retry completes. An async waiter registers
/// its waker while checking the report so completion cannot be missed.
#[derive(Default)]
pub(in crate::v02::resource::recovery) struct AttemptObservation {
    /// Completed attempt report made available to every observer.
    report: Mutex<Option<crate::v02::report::RecoveryAttemptReport>>,
    /// Waker for the current async waiter, if it has not observed completion.
    waker: Mutex<Option<std::task::Waker>>,
}

/// Publishes a completed retry report and wakes a registered async waiter.
///
/// The recovery snapshot also provides the failure list copied into the
/// observer-facing report.
///
/// # Parameters
///
/// * `observation` — shared report and waiter state for the retry.
/// * `snapshot` — final recovery snapshot produced by the attempt.
pub(in crate::v02::resource::recovery) fn publish_attempt(
    observation: &AttemptObservation,
    snapshot: RecoverySnapshot,
) {
    *lock(&observation.report) = Some(crate::v02::report::RecoveryAttemptReport {
        failures: Arc::clone(&snapshot.last_failures),
        recovery: snapshot,
    });
    if let Some(waker) = lock(&observation.waker).take() {
        waker.wake();
    }
}

/// Waits for the shared retry report without blocking the executor thread.
///
/// The future checks the report before and after registering its waker so a
/// concurrent publication cannot leave the waiter asleep.
///
/// # Parameters
///
/// * `observation` — shared report and waiter state to observe.
///
/// # Returns
///
/// The completed report published for the retry.
pub(in crate::v02::resource::recovery) async fn wait_attempt_async(
    observation: &AttemptObservation,
) -> crate::v02::report::RecoveryAttemptReport {
    std::future::poll_fn(|cx| {
        if let Some(report) = lock(&observation.report).clone() {
            return std::task::Poll::Ready(report);
        }
        *lock(&observation.waker) = Some(cx.waker().clone());
        if let Some(report) = lock(&observation.report).clone() {
            std::task::Poll::Ready(report)
        } else {
            std::task::Poll::Pending
        }
    })
    .await
}

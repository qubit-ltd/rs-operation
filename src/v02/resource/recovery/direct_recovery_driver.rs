// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Direct retry execution that preserves cleanup responsibility on
//! cancellation.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use crate::v02::resource::recovery::AttemptObservation;
use crate::v02::resource::recovery::RecoveryRun;
use crate::v02::resource::recovery::publish_attempt;

/// Directly polls a retry while retaining responsibility across cancellation.
struct DirectRecoveryDriver {
    /// Run and owner payload retained until a result or cancellation is saved.
    run: Option<RecoveryRun>,
    /// Shared observer report and notification state.
    observation: Arc<AttemptObservation>,
    /// Pending action future, removed before its outcome is marked unconfirmed.
    future: Option<crate::v02::resource::recovery::RecoveryFuture>,
    /// Prevents Drop from publishing a second terminal transition.
    completed: bool,
}

impl DirectRecoveryDriver {
    /// Creates a direct retry driver which owns the run until it completes.
    ///
    /// # Parameters
    ///
    /// * `run` — admitted retry and retained owner state.
    /// * `observation` — shared report and notification state for observers.
    fn new(run: RecoveryRun, observation: Arc<AttemptObservation>) -> Self {
        Self {
            run: Some(run),
            observation,
            future: None,
            completed: false,
        }
    }

    /// Saves an unconfirmed result and wakes all observers after releasing core
    /// locks.
    ///
    /// # Parameters
    ///
    /// * `panic` — panic payload retained for diagnosis.
    /// * `future` — unfinished action future retained by the recovery core.
    fn complete_unconfirmed(
        &mut self,
        panic: Box<dyn std::any::Any + Send + 'static>,
        future: Option<crate::v02::resource::recovery::RecoveryFuture>,
    ) {
        if let Some(run) = self.run.as_mut() {
            run.complete_unconfirmed(panic, future);
            publish_attempt(&self.observation, run.core.snapshot());
        }
        self.completed = true;
    }
}

impl Future for DirectRecoveryDriver {
    /// The driver completes after recording the retry's final state.
    type Output = ();

    /// Constructs and polls the retry action behind unwind boundaries.
    ///
    /// # Parameters
    ///
    /// * `self` — direct retry driver retaining the run and pending action.
    /// * `context` — task context used to register the action's waker.
    ///
    /// # Returns
    ///
    /// `Pending` while cleanup is unresolved, or `Ready` after publishing a
    /// success, failure, or unconfirmed report.
    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().get_mut();
        if this.future.is_none() {
            let run = this.run.as_mut().expect("direct retry run remains present");
            let payload = run.payload.as_ref().expect("attempt owns payload").clone();
            let action = Arc::clone(run.action.as_ref().expect("attempt owns recovery action"));
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| action.retry(payload))) {
                Ok(future) => this.future = Some(future),
                Err(panic) => {
                    this.complete_unconfirmed(panic, None);
                    return Poll::Ready(());
                }
            }
        }

        let future = this.future.as_mut().expect("retry future was constructed");
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| future.as_mut().poll(context))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => {
                let future = this.future.take().expect("completed future remains present");
                if let Err(panic) = super::recovery_attempt::drop_completed_future(future) {
                    if let Err(secondary) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(result))) {
                        crate::v02::manager::core::dispose_panic_payload(secondary);
                    }
                    this.complete_unconfirmed(panic, None);
                    return Poll::Ready(());
                }
                let run = this.run.as_mut().expect("direct retry run remains present");
                run.complete(result);
                publish_attempt(&this.observation, run.core.snapshot());
                this.completed = true;
                Poll::Ready(())
            }
            Err(panic) => {
                let future = this.future.take();
                this.complete_unconfirmed(panic, future);
                Poll::Ready(())
            }
        }
    }
}

impl Drop for DirectRecoveryDriver {
    /// Retains a pending action future and publishes cancellation to observers.
    ///
    /// # Parameters
    ///
    /// * `self` — direct retry driver being cancelled.
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let future = self.future.take();
        if let Some(mut run) = self.run.take() {
            run.complete_cancelled(future);
            publish_attempt(&self.observation, run.core.snapshot());
        } else if let Some(future) = future {
            std::mem::forget(future);
        }
        self.completed = true;
    }
}

/// Creates a driver that owns the direct retry's cancellation responsibility.
///
/// # Parameters
///
/// * `run` — admitted retry run and retained payload.
/// * `observation` — observer report and notification shared by the attempt.
pub(super) fn drive_direct(run: RecoveryRun, observation: Arc<AttemptObservation>) -> impl Future<Output = ()> {
    DirectRecoveryDriver::new(run, observation)
}

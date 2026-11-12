// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
// =============================================================================

#[path = "support/gate.rs"]
mod gate;

use gate::Gate;
use qubit_operation::protocol::{CleanupContext, OwnedCleanup};
use qubit_operation::report::{CleanupOutcome, OperationOutcome, StopReason, SupervisorLimits};
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct FeatureCleanup(Arc<AtomicUsize>);

impl OwnedCleanup for FeatureCleanup {
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { CleanupOutcome::Cleaned })
    }
}

#[test]
fn cancelled_inference_keeps_features_leased_until_native_worker_exit() {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let body_cleanup_calls = Arc::clone(&cleanup_calls);
    let native_exit = Gate::new();
    let native_started = native_exit.clone();
    let mut operation = supervisor
        .start(ScopeOptions::default(), move |scope| async move {
            let features = scope
                .register_resource(FeatureCleanup(body_cleanup_calls))
                .expect("feature owner registered before native start");
            let feature_use = scope
                .acquire_use(features)
                .expect("native worker holds feature lease");

            // Fake sampling completed; the native inference API itself is not interruptible.
            native_started.wait().await;
            drop(feature_use);
            Ok::<_, &'static str>("inference complete")
        })
        .expect("inference root is accepted");

    native_exit.wait_started();
    assert!(native_exit.started());
    assert!(!native_exit.is_released());
    operation.request_cancel(StopReason::UserCancelled);
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);
    native_exit.release();
    native_exit.wait_released();

    assert!(matches!(
        operation.result_blocking().expect("stop result is observable"),
        OperationOutcome::Stopped(ref stop) if stop.reason == StopReason::UserCancelled
    ));
    let settlement = operation
        .released_blocking()
        .expect("native worker and cleanup settle");
    assert_eq!(
        settlement.resource_phase,
        qubit_operation::report::ResourcePhase::Released
    );
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
}

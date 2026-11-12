// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Runs a supervisor-managed asynchronous operation and inspects its receipt,
//! resource settlement, and shutdown result.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Wake;
use std::task::Waker;
use std::time::Duration;

use qubit_operation::report::OperationOutcome;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;

/// Waker adapter that resumes a parked thread when the future notifies it.
struct ThreadWaker(std::thread::Thread);
impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Drive a future on the current thread by parking until its waker unparks it.
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
        std::thread::park();
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let (outcome, settlement) = block_on(supervisor.run_to_completion(ScopeOptions::default(), |_scope| async {
        Ok::<_, &'static str>("receipt-17")
    }))?;
    match outcome {
        OperationOutcome::Succeeded(receipt) => println!("stored receipt {receipt}"),
        other => return Err(format!("pipeline did not succeed: {other:?}").into()),
    }
    println!("all resources released: {}", settlement.all_released);
    let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    println!("shutdown pending requests: {}", shutdown.pending_requests);
    Ok(())
}

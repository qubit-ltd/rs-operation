// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::mpsc;
use std::time::Duration;

use qubit_operation::driver::ThreadExecutor;
use qubit_operation::report::OperationOutcome;
use qubit_operation::report::StopReason;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let executor = ThreadExecutor::new(1, 1)?;
    let (entered_tx, entered_rx) = mpsc::channel();
    let (finish_tx, finish_rx) = mpsc::channel();
    let mut inference = supervisor.start_blocking(&executor, ScopeOptions::default(), move |_scope| {
        entered_tx.send(()).expect("caller is waiting");
        finish_rx.recv().expect("caller releases simulated native call");
        Ok::<_, &'static str>("unused result")
    })?;

    entered_rx.recv()?;
    inference.request_cancel(StopReason::UserCancelled);
    println!("cancel requested; blocking inference still owns its worker slot");
    finish_tx.send(())?;

    match inference.result_blocking()? {
        OperationOutcome::Stopped(report) => {
            println!("stopped after call returned: {:?}", report.reason)
        }
        other => return Err(format!("unexpected inference outcome: {other:?}").into()),
    }
    let settlement = inference.released_blocking()?;
    println!("resource phase: {:?}", settlement.resource_phase);
    let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    println!("shutdown pending requests: {}", shutdown.pending_requests);
    Ok(())
}

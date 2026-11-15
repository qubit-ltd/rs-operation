// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_operation::OperationOutcome as SubmissionOutcome;
use qubit_operation::ShutdownMode;

include!("../doc/examples-0.2.0/submission_context.rs");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = OperationManager::new(Default::default())?;
    let mut root = submit_scan(&manager, b"root scan".to_vec())?;
    println!("root scan outcome: {:?}", root.wait_outcome()?);
    println!("root scan released: {}", root.wait_settlement()?.all_released());

    let mut request = submit_scan_request(&manager, b"nested scan".to_vec())?;
    match request.wait_outcome()? {
        SubmissionOutcome::Succeeded(id) => println!("request: {id}"),
        other => eprintln!("request failed: {other:?}"),
    }
    println!("request released: {}", request.wait_settlement()?.all_released());
    let shutdown_report = manager.request_shutdown(ShutdownMode::Drain).wait()?;
    assert_eq!(shutdown_report.snapshot.active_operations, 0);
    Ok(())
}

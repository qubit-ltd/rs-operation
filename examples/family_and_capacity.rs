// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_operation::OperationOutcome as CapacityDemoOutcome;
use qubit_operation::ShutdownMode;

include!("../doc/examples-0.2.0/family_and_capacity.rs");

/// Runs the parent/child capacity demonstration for one resource limit.
///
/// The helper reports the final outcome and settlement, then drains the
/// manager.
fn run_case(resource_limit: usize) -> Result<CapacityDemoOutcome<(), String>, Box<dyn std::error::Error>> {
    let manager = create_capacity_demo_manager(resource_limit).map_err(std::io::Error::other)?;
    let mut handle = submit_capacity_demo(&manager, resource_limit)?.into_sync();
    let outcome = handle.wait_outcome()?;
    let settlement = handle.wait_settlement()?;
    println!(
        "resource limit {resource_limit}; released: {}",
        settlement.all_released()
    );
    let shutdown_report = manager.as_sync().request_shutdown(ShutdownMode::Drain).wait()?;
    assert_eq!(shutdown_report.snapshot.active_operations, 0);
    Ok(outcome)
}

/// Runs the managed-byte summary example and reports its settlement status.
///
/// This verifies both the detected file summary and cleanup before shutdown.
fn run_summary() -> Result<(), Box<dyn std::error::Error>> {
    let manager = create_capacity_demo_manager(3).map_err(std::io::Error::other)?;
    let mut handle = submit_file_summary(&manager, b"%PDF-1.7\n".to_vec())?.into_sync();
    match handle.wait_outcome()? {
        CapacityDemoOutcome::Succeeded(summary)
            if summary.byte_count == 9 && summary.mime == Some("application/pdf") =>
        {
            println!("summary: {} bytes, {:?}", summary.byte_count, summary.mime);
        }
        other => {
            return Err(std::io::Error::other(format!("unexpected summary outcome: {other:?}")).into());
        }
    }
    let settlement = handle.wait_settlement()?;
    println!("summary resources released: {}", settlement.all_released());
    let shutdown_report = manager.as_sync().request_shutdown(ShutdownMode::Drain).wait()?;
    assert_eq!(shutdown_report.snapshot.active_operations, 0);
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_summary()?;
    match run_case(2)? {
        CapacityDemoOutcome::Failed(_) | CapacityDemoOutcome::ChildFailed(_) => {
            println!("resource limit 2: child registration rejected as expected")
        }
        other => {
            return Err(std::io::Error::other(format!("expected capacity failure, got {other:?}")).into());
        }
    }
    match run_case(3)? {
        CapacityDemoOutcome::Succeeded(()) => {
            println!("resource limit 3: child completed")
        }
        other => {
            return Err(std::io::Error::other(format!("expected child success, got {other:?}")).into());
        }
    }
    Ok(())
}

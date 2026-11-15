// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use qubit_operation::ManagerOptions;
use qubit_operation::OperationOutcome as InferenceOutcome;
use qubit_operation::ShutdownMode;

include!("../doc/examples-0.2.0/inference.rs");

struct DemoModel;

impl ModelBackend for DemoModel {
    fn infer(&self, features: &[f32]) -> Result<Vec<f32>, String> {
        Ok(vec![features.iter().sum()])
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = AsyncOperationManager::new(ManagerOptions::default())?;
    let handle = submit_inference(&manager, Arc::new(DemoModel), vec![1.0, 2.0, 3.0])?;
    let mut handle = handle.into_sync();
    match handle.wait_outcome()? {
        InferenceOutcome::Succeeded(values) => println!("prediction: {values:?}"),
        other => eprintln!("inference failed: {other:?}"),
    }
    let settlement = handle.wait_settlement()?;
    println!("all resources released: {}", settlement.all_released());
    let shutdown_report = manager.as_sync().request_shutdown(ShutdownMode::Drain).wait()?;
    assert_eq!(shutdown_report.snapshot.active_operations, 0);
    Ok(())
}

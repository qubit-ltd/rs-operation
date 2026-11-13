// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use qubit_operation::protocol::CleanupContext;
use qubit_operation::protocol::OwnedCleanup;
use qubit_operation::report::CleanupOutcome;
use qubit_operation::report::OperationOutcome;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;

// 应用提供的清理对象，持有清理所需的数据。
struct StagedInput(Arc<Vec<u8>>);

impl OwnedCleanup for StagedInput {
    fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        Box::pin(async move {
            drop(self.0);
            CleanupOutcome::Cleaned
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let mut detection = supervisor.start(ScopeOptions::default(), |scope| async move {
        let bytes = Arc::new(b"%PDF-1.7\n".to_vec());
        let resource = scope
            .register_resource(StagedInput(Arc::clone(&bytes)))
            .map_err(|_| "cannot register input cleanup")?;
        let input_use = scope.acquire_use(resource).map_err(|_| "input is closing")?;
        let mime = bytes.starts_with(b"%PDF-").then_some("application/pdf");
        drop(bytes);
        drop(input_use);
        Ok::<Option<&'static str>, &'static str>(mime)
    })?;

    // 即使业务失败，也等待收尾完成。
    let outcome = detection.result_blocking();
    let settlement = detection.released_blocking();
    let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));

    match outcome? {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        OperationOutcome::Succeeded(None) => println!("MIME: no match"),
        other => eprintln!("detection outcome: {other:?}"),
    }
    let report = settlement?;
    if report.all_released {
        println!("input released");
    } else {
        eprintln!(
            "input settlement: {:?}; failures: {:?}",
            report.resource_phase, report.cleanup_failures
        );
    }
    if !shutdown.is_complete() {
        eprintln!("shutdown pending: {shutdown:?}");
    }
    Ok(())
}

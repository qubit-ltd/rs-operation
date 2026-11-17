// Copyright (c) 2026 Haixing Hu.
// SPDX-License-Identifier: Apache-2.0
//! Minimal application-owned service integration used by the developer guide.

use qubit_operation::{
    ManagerOptions, OperationManager, OperationOptions, OperationOutcome, SettlementReport,
    ShutdownMode,
};
use std::sync::Arc;

// Application-owned service boundary; no HTTP or MIME library is implied.
struct InspectionService {
    manager: OperationManager,
}

struct Inspection {
    mime: Option<&'static str>,
    settlement: Arc<SettlementReport>,
}

impl InspectionService {
    fn new() -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            manager: OperationManager::new(ManagerOptions::default())?,
        })
    }

    fn inspect(&self, bytes: Vec<u8>) -> Result<Inspection, Box<dyn std::error::Error>> {
        let mut handle = self
            .manager
            .try_submit(OperationOptions::default(), move |scope| {
                let input = scope.manage(bytes).map_err(|error| {
                    let (kind, bytes) = error.into_parts();
                    drop(bytes); // Vec needs no remote or asynchronous cleanup.
                    format!("input registration failed: {kind:?}")
                })?;
                Ok::<_, String>(input.starts_with(b"%PDF-").then_some("application/pdf"))
            })?;
        // This result owns no ResourceRef; either branch can now be settled.
        let outcome = handle.wait_outcome()?;
        let settlement = handle.wait_settlement()?;
        match outcome {
            OperationOutcome::Succeeded(mime) => Ok(Inspection { mime, settlement }),
            other => Err(std::io::Error::other(format!(
                "inspection failed: {other:?}; settlement: {:?}",
                settlement.status
            ))
            .into()),
        }
    }

    fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
        let report = self.manager.request_shutdown(ShutdownMode::Drain).wait()?;
        println!("shutdown: {:?}", report.snapshot);
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let service = InspectionService::new()?;
    for bytes in [b"%PDF-1.7\n".to_vec(), b"plain text".to_vec()] {
        let result = service.inspect(bytes)?;
        println!(
            "MIME: {:?}; released: {}",
            result.mime,
            result.settlement.all_released()
        );
    }
    service.shutdown()?;
    Ok(())
}

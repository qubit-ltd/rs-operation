use std::time::Duration;

use qubit_operation::report::OperationOutcome;
use qubit_operation::report::SupervisorLimits;
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::OperationSupervisor;
use qubit_operation::supervisor::ShutdownMode;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let mut operation = supervisor.start(ScopeOptions::default(), |_scope| async {
        // A real application would download, transform, and publish owned data here.
        Ok::<_, &'static str>("artifact-42")
    })?;

    // The business result and resource settlement are distinct observations.
    let outcome = operation.result_blocking()?;
    match outcome {
        OperationOutcome::Succeeded(id) => println!("published {id}"),
        other => return Err(format!("pipeline did not succeed: {other:?}").into()),
    }
    let settled = operation.released_blocking()?;
    println!("resource phase: {:?}", settled.resource_phase);

    let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    println!("shutdown pending requests: {}", shutdown.pending_requests);
    Ok(())
}

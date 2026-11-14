include!("../doc/examples-0.2.0/family_and_capacity.rs");

use qubit_operation::{OperationOutcome, ShutdownMode};

fn run_case(
    resource_limit: usize,
) -> Result<OperationOutcome<(), String>, Box<dyn std::error::Error>> {
    let manager = create_capacity_demo_manager(resource_limit).map_err(std::io::Error::other)?;
    let mut handle = submit_capacity_demo(&manager, resource_limit)?.into_sync();
    let outcome = handle.wait_outcome()?;
    let settlement = handle.wait_settlement()?;
    println!(
        "resource limit {resource_limit}; released: {}",
        settlement.all_released()
    );
    manager
        .as_sync()
        .request_shutdown(ShutdownMode::Drain)
        .wait()?;
    Ok(outcome)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    match run_case(2)? {
        OperationOutcome::Failed(_) | OperationOutcome::ChildFailed(_) => {
            println!("resource limit 2: child registration rejected as expected")
        }
        other => {
            return Err(
                std::io::Error::other(format!("expected capacity failure, got {other:?}")).into(),
            );
        }
    }
    match run_case(3)? {
        OperationOutcome::Succeeded(()) => println!("resource limit 3: child completed"),
        other => {
            return Err(
                std::io::Error::other(format!("expected child success, got {other:?}")).into(),
            );
        }
    }
    Ok(())
}

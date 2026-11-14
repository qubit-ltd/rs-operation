include!("../doc/examples-0.2.0/family_and_capacity.rs");

use qubit_operation::ShutdownMode;

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

fn run_summary() -> Result<(), Box<dyn std::error::Error>> {
    let manager = create_capacity_demo_manager(3).map_err(std::io::Error::other)?;
    let mut handle = submit_file_summary(&manager, b"%PDF-1.7\n".to_vec())?.into_sync();
    match handle.wait_outcome()? {
        qubit_operation::OperationOutcome::Succeeded(summary)
            if summary.byte_count == 9 && summary.mime == Some("application/pdf") =>
        {
            println!("summary: {} bytes, {:?}", summary.byte_count, summary.mime);
        }
        other => {
            return Err(
                std::io::Error::other(format!("unexpected summary outcome: {other:?}")).into(),
            );
        }
    }
    let settlement = handle.wait_settlement()?;
    println!("summary resources released: {}", settlement.all_released());
    manager
        .as_sync()
        .request_shutdown(ShutdownMode::Drain)
        .wait()?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_summary()?;
    match run_case(2)? {
        qubit_operation::OperationOutcome::Failed(_)
        | qubit_operation::OperationOutcome::ChildFailed(_) => {
            println!("resource limit 2: child registration rejected as expected")
        }
        other => {
            return Err(
                std::io::Error::other(format!("expected capacity failure, got {other:?}")).into(),
            );
        }
    }
    match run_case(3)? {
        qubit_operation::OperationOutcome::Succeeded(()) => {
            println!("resource limit 3: child completed")
        }
        other => {
            return Err(
                std::io::Error::other(format!("expected child success, got {other:?}")).into(),
            );
        }
    }
    Ok(())
}

include!("../doc/examples-0.2.0/inference.rs");

use qubit_operation::ManagerOptions;
use qubit_operation::ShutdownMode;

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
        qubit_operation::OperationOutcome::Succeeded(values) => println!("prediction: {values:?}"),
        other => eprintln!("inference failed: {other:?}"),
    }
    let settlement = handle.wait_settlement()?;
    println!("all resources released: {}", settlement.all_released());
    manager.as_sync().request_shutdown(ShutdownMode::Drain).wait()?;
    Ok(())
}

use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;

fn main() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let _operation = manager
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
        .unwrap();
}

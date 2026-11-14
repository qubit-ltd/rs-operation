use qubit_operation::AdmissionError;
use qubit_operation::OperationHandle;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;

pub fn submit_scan(
    manager: &OperationManager,
    bytes: Vec<u8>,
) -> Result<OperationHandle<usize, String>, AdmissionError> {
    manager
        .try_submit(OperationOptions::default(), move |scope| {
            let input = scope.manage(bytes).map_err(|error| format!("{:?}", error.kind()))?;
            Ok::<_, String>(input.len())
        })
        .map_err(|error| error.into_parts().0)
}

pub fn submit_scan_request(
    manager: &OperationManager,
    bytes: Vec<u8>,
) -> Result<OperationHandle<&'static str, String>, AdmissionError> {
    manager
        .try_submit(OperationOptions::default(), move |scope| {
            let child = submit_scan(&scope.manager(), bytes).map_err(|e| e.to_string())?;
            let _observer = child.into_observer();
            Ok::<_, String>("document-42")
        })
        .map_err(|error| error.into_parts().0)
}

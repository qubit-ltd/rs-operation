use qubit_operation::{
    AdmissionError, AsyncOperationHandle, AsyncOperationManager, ManagerOptions, OperationOptions,
    OperationOutcome,
};

#[derive(Debug)]
struct FileSummary {
    byte_count: usize,
    mime: Option<&'static str>,
}

fn submit_file_summary(
    manager: &AsyncOperationManager,
    bytes: Vec<u8>,
) -> Result<AsyncOperationHandle<FileSummary, String>, AdmissionError> {
    manager
        .try_submit(
            OperationOptions::default(),
            move |parent_scope| async move {
                let input = parent_scope.manage(bytes).map_err(|e| e.to_string())?;
                let child = parent_scope
                    .manager()
                    .try_submit(OperationOptions::default(), move |child_scope| {
                        let header_bytes = input.iter().take(5).copied().collect::<Vec<u8>>();
                        let header = child_scope
                            .manage(header_bytes)
                            .map_err(|e| e.to_string())?;
                        Ok::<_, String>(FileSummary {
                            byte_count: input.len(),
                            mime: (header.as_slice() == b"%PDF-").then_some("application/pdf"),
                        })
                    })
                    .map_err(|e| e.to_string())?;
                let mut child = child.into_async();
                match child.wait_outcome().await.map_err(|e| e.to_string())? {
                    OperationOutcome::Succeeded(summary) => Ok(summary),
                    other => Err(format!("summary child failed: {other:?}")),
                }
            },
        )
        .map_err(|error| error.into_parts().0)
}

fn create_capacity_demo_manager(resource_limit: usize) -> Result<AsyncOperationManager, String> {
    let mut options = ManagerOptions::default();
    options.scope_limits.child_operations = 2;
    options.scope_limits.resources = resource_limit;
    AsyncOperationManager::new(options).map_err(|e| e.to_string())
}

fn submit_capacity_demo(
    manager: &AsyncOperationManager,
    resource_limit: usize,
) -> Result<AsyncOperationHandle<(), String>, AdmissionError> {
    let mut options = OperationOptions::default();
    options.limits.child_operations = 2;
    options.limits.resources = resource_limit;
    manager
        .try_submit(options, |parent_scope| async move {
            let input = parent_scope
                .manage(vec![1_u8, 2, 3])
                .map_err(|e| e.to_string())?;
            let _request_metadata = parent_scope
                .manage(String::from("document-42"))
                .map_err(|e| e.to_string())?;
            let child = parent_scope
                .manager()
                .try_submit(OperationOptions::default(), move |child_scope| {
                    let _header = child_scope
                        .manage(input.iter().take(2).copied().collect::<Vec<_>>())
                        .map_err(|e| e.to_string())?;
                    Ok::<(), String>(())
                })
                .map_err(|e| e.to_string())?;
            let mut child = child.into_async();
            match child.wait_outcome().await.map_err(|e| e.to_string())? {
                OperationOutcome::Succeeded(()) => Ok(()),
                other => Err(format!("capacity demo child failed: {other:?}")),
            }
        })
        .map_err(|error| error.into_parts().0)
}

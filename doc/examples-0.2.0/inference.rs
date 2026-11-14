use qubit_operation::{
    AdmissionError, AsyncOperationHandle, AsyncOperationManager, OperationOptions,
    OperationOutcome, ResourceScopeOptions,
};
use std::sync::Arc;

pub trait ModelBackend: Send + Sync + 'static {
    fn infer(&self, features: &[f32]) -> Result<Vec<f32>, String>;
}

pub fn submit_inference(
    manager: &AsyncOperationManager,
    model: Arc<dyn ModelBackend>,
    features: Vec<f32>,
) -> Result<AsyncOperationHandle<Vec<f32>, String>, AdmissionError> {
    manager
        .try_submit(OperationOptions::default(), move |scope| async move {
            let child = {
                let inputs = scope
                    .resource_scope(ResourceScopeOptions { capacity: 1 })
                    .map_err(|e| e.to_string())?;
                let input = inputs.manage(features).map_err(|e| e.to_string())?;
                scope
                    .manager()
                    .try_submit(OperationOptions::default(), move |_child_scope| {
                        model.infer(&input)
                    })
                    .map_err(|e| e.to_string())?
            }; // inputs 结束；input 已移给同步子操作，仍然存活。

            let mut child = child.into_async();
            match child.wait_outcome().await.map_err(|e| e.to_string())? {
                OperationOutcome::Succeeded(prediction) => Ok(prediction),
                other => Err(format!("model outcome: {other:?}")),
            }
        })
        .map_err(|error| error.into_parts().0)
}

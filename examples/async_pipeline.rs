use qubit_operation::report::{OperationOutcome, SupervisorLimits};
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};
use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};
use std::time::Duration;

struct ThreadWaker(std::thread::Thread);
impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
        std::thread::park();
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let (outcome, settlement) = block_on(
        supervisor.run_to_completion(ScopeOptions::default(), |_scope| async {
            Ok::<_, &'static str>("receipt-17")
        }),
    )?;
    match outcome {
        OperationOutcome::Succeeded(receipt) => println!("stored receipt {receipt}"),
        other => return Err(format!("pipeline did not succeed: {other:?}").into()),
    }
    println!("all resources released: {}", settlement.all_released);
    let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    println!("shutdown pending requests: {}", shutdown.pending_requests);
    Ok(())
}

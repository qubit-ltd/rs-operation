// EXPECT: request_stop
use qubit_operation::{OperationObserver, StopReason};
fn check(observer: OperationObserver) {
    let _ = observer.request_stop(StopReason::User);
}

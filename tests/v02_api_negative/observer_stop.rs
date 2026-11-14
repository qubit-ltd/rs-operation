// EXPECT: request_stop
use qubit_operation::OperationObserver;
use qubit_operation::StopReason;
fn check(observer: OperationObserver) {
    let _ = observer.request_stop(StopReason::User);
}

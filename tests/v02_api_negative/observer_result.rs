// EXPECT: try_take_outcome
use qubit_operation::OperationObserver;
fn check(observer: OperationObserver) {
    let _ = observer.try_take_outcome();
}

// EXPECT: try_submit
use qubit_operation::OperationScope;
fn check(scope: &OperationScope) {
    let _ = scope.try_submit(|_| Ok::<_, ()>(()));
}

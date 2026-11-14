// EXPECT: Clone
use qubit_operation::OperationHandle;
fn requires_clone<T: Clone>() {}
fn check() {
    requires_clone::<OperationHandle<(), ()>>();
}

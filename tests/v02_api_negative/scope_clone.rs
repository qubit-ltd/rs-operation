// EXPECT: Clone
use qubit_operation::OperationScope;
fn requires_clone<T: Clone>() {}
fn check() {
    requires_clone::<OperationScope>();
}

// EXPECT: DerefMut
use qubit_operation::ResourceRef;
fn check(mut resource: ResourceRef<std::sync::Mutex<usize>>) {
    *resource = std::sync::Mutex::new(3);
}

use qubit_operation::ResourceRef;

fn mutate(resource: &mut ResourceRef<String>) {
    resource.push_str("forbidden");
}

fn main() {}

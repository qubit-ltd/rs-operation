# Qubit Operation

[![Rust CI](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-operation/coverage-badge.json)](https://qubit-ltd.github.io/rs-operation/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-operation.svg?color=blue)](https://crates.io/crates/qubit-operation)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-operation` coordinates business outcomes and resource cleanup across parent and child operations. Rust application developers can use the same lifecycle model through synchronous and asynchronous managers, while each scope owns its resources and child work. This is useful when cancellation or an early result must not release data still used by background work.

## Scenario: inspect an uploaded document

A service can register uploaded bytes with an operation scope, inspect a PDF signature, then observe the business result separately from resource settlement. The application still owns file staging, MIME policy, external commands, and recovery decisions.

Until `qubit-operation` 0.2 is published, point your application at the local `rs-operation` checkout. Replace the placeholder with its actual path:

```toml
[dependencies]
qubit-operation = { path = "<path-to-rs-operation>" }
```

After the crate is published, you can use `qubit-operation = "^0.2"` from crates.io. Its own dependencies are declared in the local crate manifest.

This small runnable path checks a memory buffer. Save it as `src/main.rs` and run `cargo run`:

```rust
use qubit_operation::{ManagerOptions, OperationManager, OperationOptions, OperationOutcome};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = OperationManager::new(ManagerOptions::default())?;
    let mut operation = manager.try_submit(OperationOptions::default(), |scope| {
        let input = scope.manage(b"%PDF-1.7\n".to_vec()).map_err(|error| format!("{:?}", error.kind()))?;
        Ok::<_, String>(input.starts_with(b"%PDF-").then_some("application/pdf"))
    })?;

    let outcome = operation.wait_outcome()?;
    let settlement = operation.wait_settlement()?;
    match outcome {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        OperationOutcome::Succeeded(None) => println!("MIME: no match"),
        other => eprintln!("detection failed: {other:?}"),
    }
    println!("all resources released: {}", settlement.all_released());
    manager.request_shutdown(qubit_operation::ShutdownMode::Drain).wait()?;
    Ok(())
}
```

`wait_outcome` reports the business result; `wait_settlement` reports cleanup and recovery state. Neither fact implies the other. For real pipelines, each background or asynchronous user must hold its `ResourceRef` until it stops accessing the resource. `scope.manager()` submits children under the current operation, so their results and settlement remain part of the parent lifecycle.

The crate does not detect MIME types, provide file or command adapters, or implement business retries, compensation, and distributed recovery. Shared resource access is not exclusive: synchronization belongs to the resource type or application. See the developer guide for ownership, capacity, failure, and shutdown details.

## Learn More

- [0.2.0 Developer Guide](doc/developer_guide-0.2.0.md) · [中文开发者手册](doc/developer_guide-0.2.0.zh_CN.md) · [0.2 API 设计](doc/design-0.2.0.zh_CN.md)
- Migration: [0.1 to 0.2 (English)](doc/migration-0.1-to-0.2.md) · [中文迁移指南](doc/migration-0.1-to-0.2.zh_CN.md)
- [0.2 adapter integration guide (Chinese)](doc/adapter-guide.zh_CN.md)
- Historical 0.1 user guides (describe the incompatible 0.1 API): [English](doc/user_guide.md) · [中文](doc/user_guide.zh_CN.md)
- [API reference](https://docs.rs/qubit-operation)
- Runnable 0.2 examples: [file detection](examples/detect_file.rs), [inference](examples/inference.rs), [parent and capacity](examples/family_and_capacity.rs), [submission context](examples/submission_context.rs)
- [中文 README](README.zh_CN.md)

## Testing

```bash
cargo check --examples --locked
cargo test --locked
```

## License

Copyright (c) 2026. Haixing Hu. All rights reserved.

Licensed under the [Apache License 2.0](LICENSE).

## Contributing

Contributions are welcome. Keep public behavior and both language versions of the documentation aligned. Run `.infra/bin/align-ci.sh` and `.infra/bin/ci-check.sh` before submitting changes.

## Author

**Haixing Hu** — *Qubit Co. Ltd.*

Repository: [qubit-ltd/rs-operation](https://github.com/qubit-ltd/rs-operation)

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

Add `qubit-operation` 0.3 to the application:

```toml
[dependencies]
qubit-operation = "^0.3"
```

The crate declares its own dependencies in its manifest.

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

`wait_outcome` reports the business result; `wait_settlement` reports cleanup and recovery state. Neither fact implies the other. If either `T` or `E` (including an indirectly held object) contains a `ResourceRef` that settlement awaits, use the two-step handle API: consume and drop the result before waiting for settlement. The `run_and_settle` convenience method is for outcomes that do not retain such references. Settlement report counts describe the local scope; check its combined `status` and `all_released()`, then inspect `external_settlement` for external recovery handles and effects. Do not schedule duplicate cleanup for resources recorded at both levels. See the [resource-valued result example](doc/examples-0.3.0/inference.rs) and [0.2 to 0.3 migration guide](doc/migration-0.2-to-0.3.md) for report struct-literal changes.

For real pipelines, each background or asynchronous user must hold its `ResourceRef` until it stops accessing the resource. `scope.manager()` submits children under the current operation, so their results and settlement remain part of the parent lifecycle.

The crate does not detect MIME types, provide file or command adapters, or implement business retries, compensation, and distributed recovery. External adapters implement `PreparedExternalOperation` and report typed outcome separately from task release; the manager retains resource owners until users return their references. For synchronous managers, external adapters use `async_executor` capacity and worker threads, while ordinary synchronous `try_submit` jobs continue to use the synchronous pool. Keep adapter callbacks short and non-blocking. The complete [simulated provider lifecycle](doc/examples-0.3.0/provider_read.rs) demonstrates these responsibilities; [leased_temp.rs](doc/examples-0.3.0/leased_temp.rs) is a helper excerpt. See the [0.3.0 developer guide](doc/developer_guide-0.3.0.md) for adapter, ownership, capacity, failure, and shutdown details.

## Learn More

- [0.3.0 Developer Guide](doc/developer_guide-0.3.0.md) · [中文开发者手册](doc/developer_guide-0.3.0.zh_CN.md) · [0.2 API 设计](doc/design-0.2.0.zh_CN.md)
- Migration: [0.2 to 0.3 (English)](doc/migration-0.2-to-0.3.md) · [中文迁移指南](doc/migration-0.2-to-0.3.zh_CN.md)
- [0.2 adapter integration guide (Chinese)](doc/adapter-guide.zh_CN.md)
- Historical 0.1 user guides (describe the incompatible 0.1 API): [English](doc/user_guide.md) · [中文](doc/user_guide.zh_CN.md)
- [API reference](https://docs.rs/qubit-operation)
- Runnable 0.2 examples: [file detection](examples/detect_file.rs), [inference](examples/inference.rs), [parent and capacity](examples/family_and_capacity.rs), [submission context](examples/submission_context.rs)
- 0.3 adapter lifecycle examples: [complete simulated provider lifecycle](doc/examples-0.3.0/provider_read.rs), [resource-valued outcome](doc/examples-0.3.0/inference.rs), [stop/settlement helper excerpt](doc/examples-0.3.0/leased_temp.rs)
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

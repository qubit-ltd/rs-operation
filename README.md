# Qubit Operation

[![Rust CI](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-operation/coverage-badge.json)](https://qubit-ltd.github.io/rs-operation/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-operation.svg?color=blue)](https://crates.io/crates/qubit-operation)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-operation` provides bounded operation lifecycle management for Rust services. It coordinates synchronous jobs, async tasks, parent/child operations, and external providers, while reporting business outcomes separately from resource settlement. It is intended for developers who need cancellation and background work without losing track of unfinished cleanup.

Consider an upload service that hands a document to a background detector. The client may cancel while the detector is still reading the input. Treating “request ended” as “input can be released” confuses business completion with resource use. The crate keeps resource ownership in a scope and tracks actual users through `ResourceRef`, allowing an early outcome followed by cleanup. The application still owns its upload protocol, detection algorithm, and recovery policy.

## Installation

Requires Rust 1.94 or later; the crate uses the Rust 2024 edition. Add this to your application's `Cargo.toml`:

```toml
[dependencies]
qubit-operation = "^0.3"
```

## Quick Start

This starter exercise checks an uploaded document's PDF signature: create a manager, submit an inspection, register the input, consume the outcome, observe settlement, and shut down. Save it as `src/main.rs` and run `cargo run`:

```rust
use qubit_operation::{ManagerOptions, OperationManager, OperationOptions, OperationOutcome};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = OperationManager::new(ManagerOptions::default())?;
    let mut operation = manager.try_submit(OperationOptions::default(), |scope| {
        let input = scope.manage(b"%PDF-1.7\n".to_vec()).map_err(|error| format!("{:?}", error.kind()))?;
        Ok::<_, String>(input.starts_with(b"%PDF-").then_some("application/pdf"))
    })?;

    let outcome = operation.wait_outcome()?;
    match outcome {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        OperationOutcome::Succeeded(None) => println!("MIME: no match"),
        other => eprintln!("detection failed: {other:?}"),
    }
    let settlement = operation.wait_settlement()?;
    println!("all resources released: {}", settlement.all_released());
    let _shutdown = manager.request_shutdown(qubit_operation::ShutdownMode::Drain).wait()?;
    Ok(())
}
```

A normal run prints `MIME: application/pdf` and `all resources released: true`. A different signature produces `MIME: no match`: inspection completed without a match, so the application can reject the upload or try another detector. A submission error means work was not admitted; receiving a handle still requires checking its outcome. This example checks a fixed prefix, not complete MIME detection or file safety.

The example consumes the outcome before waiting for settlement. If a returned value or error holds a `ResourceRef`, release those references before awaiting settlement to avoid waiting on yourself. See the [developer guide](doc/developer_guide-0.3.0.md) for service integration and complete failure handling.

## Why This Project Exists

An async result or cancellation flag answers only part of the lifecycle question. Neither alone establishes whether work was admitted, how it ended, who still uses its inputs, or who owns cleanup after a failure. External providers and child tasks make those facts particularly likely to diverge.

The crate gives them a shared lifecycle: reserve capacity before admission, track accepted work and scoped resources, consume each typed result once, clean up only after use ends, and retain recovery responsibility when release cannot be confirmed. Applications can handle business responses and cleanup separately without treating a returned result, stop request, or dropped handle as release evidence.

This fits upload pipelines, batch inspection, inference requests, and other services with task/resource dependencies. A short function without composition, cancellation, or deferred cleanup may need only an ordinary function or future.

## What It Provides

| Capability | Purpose |
| --- | --- |
| Sync and async managers | Admit and observe different execution styles through a shared coordinator. |
| Bounded admission and service groups | Enforce manager, parent, and group limits; return the original closure or prepared adapter on rejection. |
| Parent/child composition | Track required and optional children and configure sibling behavior after failure. |
| Scoped resources | Reserve before creation, defer cleanup through strong user references, and supply async cleanup policies. |
| Separate outcome and settlement | Consume a typed outcome, then inspect children, cleanup, external reports, and recovery responsibility. |
| External-operation protocol | Integrate work owned by another runtime, with independent outcome, input-release, and domain-settlement facts. |
| Stop, budgets, and shutdown | Propagate cooperative stops, inherit clock/deadline/budget policies, and observe drain or cancel progress. |
| Recovery and bounded evidence | Inspect cleanup failures, request recovery attempts, or hand responsibility to a `RecoveryStore`. |

The crate does not supply MIME algorithms, ready-made file/command/provider adapters, business retry policies, rollback, compensation, or distributed persistent recovery. Stop does not forcibly terminate arbitrary code. Applications synchronize mutable resource contents. Bounded capacities limit entries and tasks, not the byte size of application resources. The guide explains these constraints and troubleshooting in detail.

## Learn More

- Integration: [Developer guide](doc/developer_guide-0.3.0.md) · [中文开发者手册](doc/developer_guide-0.3.0.zh_CN.md)
- Rationale: [Design document](doc/design-0.3.0.md) · [中文设计文档](doc/design-0.3.0.zh_CN.md)
- [API reference](https://docs.rs/qubit-operation) · [中文 README](README.zh_CN.md)
- Runnable examples: [document inspection](examples/detect_file.rs), [parent/child capacity](examples/family_and_capacity.rs), [submission context](examples/submission_context.rs), [inference](examples/inference.rs)
- External integration references: [complete simulated provider](doc/examples-0.3.0/provider_read.rs), [resource-valued outcomes](doc/examples-0.3.0/inference.rs). These compile as library targets and are not real provider adapters.

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

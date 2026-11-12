# Qubit Operation

[![Rust CI](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-operation/coverage-badge.json)](https://qubit-ltd.github.io/rs-operation/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-operation.svg?color=blue)](https://crates.io/crates/qubit-operation)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-operation` gives Rust applications a bounded supervisor for owned, `Send + 'static` work, so callers can observe a business result separately from child completion and resource settlement. It is useful when a pipeline must stop cooperatively while still retaining control of work that has not yet released its resources.

## Installation

For a checkout of this repository, add the local crate to your application:

```toml
[dependencies]
qubit-operation = { path = "../rs-operation" }
```

The crate targets Rust 1.94 and has no default features or runtime dependencies. The crates.io badge shows registry status; use the path dependency while working from an unpublished checkout.

## Quick Start

This small pipeline starts owned work, checks its typed outcome, then waits for settlement. An application should put its own download, transform, and publish logic in the body and register cleanup owners through the supplied scope when it creates managed resources.

```rust,no_run
use qubit_operation::report::{OperationOutcome, SupervisorLimits};
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};
use std::time::Duration;

let supervisor = OperationSupervisor::new(SupervisorLimits::default());
let mut operation = supervisor.start(ScopeOptions::default(), |_scope| async {
    Ok::<_, &'static str>("artifact-42")
})?;
match operation.result_blocking()? {
    OperationOutcome::Succeeded(id) => println!("published {id}"),
    other => println!("pipeline outcome: {other:?}"),
}
let settlement = operation.released_blocking()?;
println!("resource phase: {:?}", settlement.resource_phase);
let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
# Ok::<(), Box<dyn std::error::Error>>(())
```

The result answers what the operation did; settlement reports whether its managed children and resources reached a terminal state. Cancellation requests cooperative stop and does not prove that a blocking or native call has returned. See the [synchronous example](examples/sync_pipeline.rs), [asynchronous example](examples/async_pipeline.rs), and [cancellation example](examples/inference_cancel.rs).

## What It Provides

- Finite root admission and per-scope child/resource registration capacities.
- One typed business outcome and repeatable observation of the final settlement report.
- Structured child scopes, use leases, cleanup dependencies, and typed recovery ownership.
- Synchronous observation and async futures over the same operation lifecycle.
- An explicit supervisor with Drain or Cancel shutdown.

The crate does not provide distributed recovery, forced termination, business retries or compensation, a progress/event integration, or a formal Tokio adapter. `PortableDriver` handles portable owned futures; runtime-bound futures need a suitable `ExternalDriver` that the application integrates with its runtime. Do not block an async runtime worker on the blocking observation methods.

## Learn More

- [中文用户指南](doc/user_guide.zh_CN.md) and [adapter integration guide](doc/adapter-guide.zh_CN.md)
- [API reference](https://docs.rs/qubit-operation)
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

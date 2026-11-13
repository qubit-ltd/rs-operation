# Qubit Operation

[![Rust CI](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-operation/coverage-badge.json)](https://qubit-ltd.github.io/rs-operation/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-operation.svg?color=blue)](https://crates.io/crates/qubit-operation)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

`qubit-operation` provides a task-level mechanism similar to Java's `try … finally …`: run business work, then retain responsibility for settling child work and cleaning up resources on success, failure, or cancellation. It supports both synchronous blocking observation and asynchronous waiting over the same lifecycle. It is useful for Rust applications that combine file I/O, external commands, or model inference.

## Think of It as a Managed `finally`

In a simple function, `finally` runs cleanup as control leaves the `try` block. Across tasks and services, returning a result or requesting cancellation does not mean that every resource is ready to be cleaned up. A process may still be reading a file, or native inference may still be using a buffer.

The library coordinates this extended cleanup responsibility:

```text
Business work (like try)
    Run the operation, start children, and use resources
Settlement (like finally, potentially continuing after the result is available)
    Wait for resource users to finish
    Run registered cleanup
    Report confirmed release or outstanding recovery responsibility
```

| Familiar idea | In this crate |
| --- | --- |
| The body of `try` | Business work submitted with `supervisor.start(...)` |
| Cleanup in `finally` | Application-defined `OwnedCleanup` registered with the scope |
| Keeping a resource alive while it is used | A use lease (`ResourceUse`) held until actual use ends |
| Return value or business failure | `result_blocking()` or `result().await` |
| Checking whether cleanup completed | `released_blocking()` or `released().await` and inspection of the report |

The supervisor retains lifecycle responsibility when the caller drops its task handle and requests cooperative stop. This is not a guarantee that cleanup always succeeds: cancellation cannot forcibly stop arbitrary native work, and cleanup can fail or remain unconfirmed. Business outcomes and resource settlement are therefore observed separately. The blocking and async interfaces expose the same lifecycle; the example below uses blocking observation.

## Scenario: detecting an uploaded file's MIME type

A service receives uploaded content, stages it in a temporary file, and runs `file` to identify its type. With media refinement enabled, it may then run `ffprobe`. This workflow comes from the [rs-mime](https://github.com/qubit-ltd/rs-mime) design.

```text
Upload → stage input → run file → optionally run ffprobe → MIME result
               └── keep input until the tools stop using it ──┘
```

Failure and cancellation complicate this workflow. If a user cancels while a process is still reading the staged file, deleting that file is premature. Returning a MIME result or dropping the result handle does not prove that processes and resources have finished shutting down either.

The application registers work and resources in a shared scope. Each input user holds a use lease (`ResourceUse`) until it actually stops using the input; the library uses those leases to coordinate cleanup. The file service stages and deletes files, the command service confirms process termination and reaping, and rs-operation coordinates their lifecycles.

## Installation

```toml
[dependencies]
qubit-operation = "^0.1"
```

Requires Rust 1.94 or later. There are no runtime dependencies or default features.

## Quick Start: detection versus input release

This runnable example preserves the important ownership relationships while replacing the staged file with a memory buffer and the external command with a PDF signature check. It needs no `file`, `ffprobe`, or model installation. It is not a complete MIME detector and does not call unimplemented rs-mime APIs.

Three objects carry the flow: `supervisor` is the application's task manager; `scope` registers this task's children and resources; `detection` is the caller's handle for results, cancellation, and settlement. `StagedInput` is an application-defined cleanup adapter, invoked by the library through `OwnedCleanup::begin`.

Save this as `src/main.rs` and run `cargo run`:

```rust
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use qubit_operation::protocol::{CleanupContext, OwnedCleanup};
use qubit_operation::report::{CleanupOutcome, OperationOutcome, ResourcePhase, SupervisorLimits};
use qubit_operation::scope::ScopeOptions;
use qubit_operation::supervisor::{OperationSupervisor, ShutdownMode};

// Like a finally action: defined by the application, scheduled by the library.
struct StagedInput(Arc<Vec<u8>>);

impl OwnedCleanup for StagedInput {
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        Box::pin(async move {
            drop(self.0); // Release the cleanup owner's reference to the buffer.
            CleanupOutcome::Cleaned
        })
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let supervisor = OperationSupervisor::new(SupervisorLimits::default());
    let mut detection = supervisor.start(ScopeOptions::default(), |scope| async move {
        // Stand in for an uploaded document, staged in memory.
        let bytes = Arc::new(b"%PDF-1.7\n".to_vec());
        let resource = scope.register_resource(StagedInput(Arc::clone(&bytes)))
            .map_err(|_| "cannot register input cleanup")?;

        // Keep the input in use while inspecting its signature.
        let input_use = scope.acquire_use(resource).map_err(|_| "input is closing")?;
        let mime = if bytes.starts_with(b"%PDF-") {
            Some("application/pdf")
        } else {
            None
        };
        drop(bytes);
        drop(input_use); // The detector no longer accesses the input.
        Ok::<Option<&'static str>, &'static str>(mime)
    })?;

    // Read the result of the business body (the try part).
    match detection.result_blocking()? {
        OperationOutcome::Succeeded(Some(mime)) => println!("MIME: {mime}"),
        other => return Err(format!("unexpected detection outcome: {other:?}").into()),
    }

    // Observe settlement after the detector has released its input lease.
    let report = detection.released_blocking()?;
    assert_eq!(report.resource_phase, ResourcePhase::Released);
    println!("input released");

    let shutdown = supervisor.shutdown_blocking(ShutdownMode::Drain, Duration::from_secs(1));
    assert_eq!(shutdown.pending_requests, 0);
    Ok(())
}
```

Expected output:

```text
MIME: application/pdf
input released
```

The example has four steps:

1. Create the input buffer and register its cleanup owner with the scope. `Arc` temporarily shares the same allocation between detection and cleanup.
2. Acquire a use lease, inspect the signature, then drop the detector's buffer reference and lease.
3. Read the business result with `result_blocking()`. `Option` distinguishes a recognized type from no match; `Result` represents errors during detection.
4. Obtain the settlement report with `released_blocking()` and check `ResourcePhase::Released` before shutting down the supervisor.

The two waits answer different questions; they do not imply a time gap. Cleanup in this small example may finish before the caller reads the business result. Production code must also handle outcomes such as `Failed` and `Stopped`, and resource phases such as `RecoveryRequired` or `Unconfirmed`. Receiving a report alone does not prove successful release.

## Connecting a real MIME pipeline

Preserve these relationships when replacing the example with staged files and external tools:

| Example step | Production integration responsibility |
| --- | --- |
| Allocate input and register `StagedInput` | The file adapter creates a staged file and registers its cleanup owner exactly once |
| Acquire `input_use` and inspect the signature | The command adapter holds the input lease while running `file` and optional `ffprobe` |
| Drop the detector's reference and lease | Return the lease only after the process and input helpers stop accessing the file |
| `OwnedCleanup::begin` | The file adapter deletes the staged file, reporting cleanup failures and recovery ownership accurately |
| Observe result and settlement separately | The application responds using the MIME result and handles unreleased resources using the settlement report |

The handle's `request_cancel` requests cooperative stop. It cannot force native inference to return or prove that a process has exited. Adapters must retain leases until input use actually ends; rs-operation cannot automatically track arbitrary file handles or external processes.

This example teaches the lifecycle protocol. An ordinary function suffices for one in-memory signature check; shared lifecycle management becomes useful when work spans services and cancellation must retain cleanup responsibility.

## Capabilities and Boundaries

- Bound root admission and per-scope child/resource registration. Domain services manage byte counts, process capacity, model memory, and other domain budgets.
- Retrieve one typed business outcome and repeatedly observe the final settlement report, with blocking and async observation APIs.
- Coordinate child scopes, use leases, and cleanup dependencies while preserving ownership needed for recovery.
- Keep shutdown under application control, with `Drain` to wait for existing work or `Cancel` to request stop and settle.

The crate does not detect MIME types, supply file or command adapters, or implement business retries, compensation, or distributed recovery. `PortableDriver` handles portable owned futures; runtime-bound futures require an application-integrated `ExternalDriver`. Do not call blocking observation methods on an async runtime worker.

## Learn More

- [English user guide](doc/user_guide.md) · [中文用户指南](doc/user_guide.zh_CN.md): full usage and error handling
- [Adapter integration guide (Chinese)](doc/adapter-guide.zh_CN.md): connecting domain services to the lifecycle protocol
- [API reference](https://docs.rs/qubit-operation)
- Runnable examples: [MIME input lifecycle](examples/mime_lifecycle.rs), [blocking observation](examples/sync_pipeline.rs), [async observation](examples/async_pipeline.rs), [cancelling a non-interruptible call](examples/inference_cancel.rs)
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

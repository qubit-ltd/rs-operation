# Qubit Operation

[![Rust CI](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-operation/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-operation/coverage-badge.json)](https://qubit-ltd.github.io/rs-operation/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-operation.svg?color=blue)](https://crates.io/crates/qubit-operation)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Managed operation lifecycles and composition for Rust applications. This repository is an initial scaffold: no public operation API is implemented and no crate has been published. The badges above do not imply an existing release or coverage result.

## Why This Project Exists

A download, conversion, and upload task spans several operations. On failure or cancellation, an application must still wait until the converter stops using temporary files before deleting them, and distinguish a known business result from released resources. This project plans to unify those lifecycle rules for synchronous and asynchronous callers.

## Development Quick Start

The empty crate can currently be built and documented. There is no implemented lifecycle API to demonstrate yet.

```bash
cargo test --locked
cargo doc --no-deps --locked
```

## Current Status

The repository contains a Rust crate scaffold, Apache-2.0 licensing, and managed shared infrastructure. Cancellation, operation scopes, resource dependencies, and supervised cleanup are planned capabilities.

The full CI coverage task currently fails because this empty library has no executable coverage data. Coverage and the Pages job that requires it become applicable when executable implementation is added; no placeholder API or artificial coverage test is included.

## Testing

```bash
./update-infra.sh --check
./.infra/bin/align-ci.sh
./.infra/bin/ci-check.sh
./.infra/bin/coverage.sh
```

## License

Copyright (c) 2026. Haixing Hu. All rights reserved.

Licensed under the [Apache License 2.0](LICENSE).

## Contributing

Contributions are welcome. Follow the Rust API guidelines, keep documentation aligned with implementation, and run `.infra/bin/align-ci.sh` and `.infra/bin/ci-check.sh` before submitting changes.

## Author

**Haixing Hu** — *Qubit Co. Ltd.*

Repository: [qubit-ltd/rs-operation](https://github.com/qubit-ltd/rs-operation)

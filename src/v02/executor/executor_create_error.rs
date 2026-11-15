// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Invalid executor configuration or failure to create a worker thread.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutorCreateError {
    /// The requested worker or queue capacity is invalid.
    InvalidOptions,
    /// The operating system rejected creation of a worker thread.
    ThreadSpawn,
}

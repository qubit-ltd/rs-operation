// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Failures returned by the blocking pool shutdown observer.

/// Why waiting for pool shutdown could not complete successfully.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShutdownError {
    /// The current worker attempted to wait for its own pool to shut down.
    WouldDeadlock,
    /// At least one worker panicked while shutting down or joining work.
    WorkerPanicked,
}

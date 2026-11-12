// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The task and reason returned when an asynchronous driver rejects work.

use super::BoxFuture;
use super::DriverError;

/// Retains the rejected task so its caller can recover or resubmit it.
pub(crate) struct DriverRejected {
    /// Identifies why the driver rejected the task.
    pub error: DriverError,
    /// The task that the driver did not accept.
    pub task: BoxFuture<()>,
}

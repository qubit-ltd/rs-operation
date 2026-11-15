// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Reserved, nonblocking ownership handoff for resource cleanup.

mod cleanup_dispatch;
mod cleanup_dispatch_permit;
mod cleanup_job;
mod cleanup_queue_error;

pub(crate) use cleanup_dispatch::CleanupDispatch;
pub(crate) use cleanup_dispatch_permit::CleanupDispatchPermit;
pub(crate) use cleanup_job::CleanupJob;
pub(crate) use cleanup_queue_error::CleanupQueueError;

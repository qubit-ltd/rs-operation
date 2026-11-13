// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;

use super::super::CleanupPermit;
use crate::report::CleanupOutcome;
use crate::report::MonotonicTime;

/// Mutable future lifecycle data for one cleanup task.
pub(in crate::resource::cleanup_scheduler) struct CleanupTaskInner {
    /// Optional deadline after which an observation reports unconfirmed.
    pub(in crate::resource::cleanup_scheduler) deadline: Option<MonotonicTime>,
    /// Whether the deadline warning has already been emitted.
    pub(in crate::resource::cleanup_scheduler) unconfirmed_reported: bool,
    /// Cleanup future, temporarily absent while it is being polled.
    pub(in crate::resource::cleanup_scheduler) future:
        Option<Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>>,
    /// Permit retained until the future settles.
    pub(in crate::resource::cleanup_scheduler) permit: Option<CleanupPermit>,
    /// Whether an observer currently owns the future for polling.
    pub(in crate::resource::cleanup_scheduler) polling: bool,
    /// Whether the cleanup future has completed or panicked.
    pub(in crate::resource::cleanup_scheduler) settled: bool,
}

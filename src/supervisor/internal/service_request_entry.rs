// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission bookkeeping for standalone and managed service requests.

use crate::context::OperationContext;

/// Tracks whether a service-group request is standalone or supervises an
/// operation.
///
/// Managed requests retain their cancellation context after startup so a
/// canceling group shutdown can stop active operations. Standalone requests
/// have no operation context to retain.
pub(in crate::supervisor) enum ServiceRequestEntry {
    /// A request lease admitted without starting a supervised operation.
    Standalone,
    /// A supervised request and its context, once the operation has started.
    Managed(
        /// The operation context after startup; `None` while startup is
        /// pending.
        Option<OperationContext>,
    ),
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Admission bookkeeping for standalone and managed service requests.

use crate::context::OperationContext;

pub(super) enum ServiceRequestEntry {
    Standalone,
    Managed(Option<OperationContext>),
}


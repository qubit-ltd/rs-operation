// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Errors returned when an external operation binding is claimed.

use crate::v02::report::ExecutionFailure;

/// Error returned when an external operation cannot claim its binding.
#[derive(Clone, Debug)]
pub enum ExternalBindError {
    /// This prepared operation has already been bound to another submission.
    AlreadyBound,
    /// Binding failed after the adapter claimed this operation.
    Failed(ExecutionFailure),
}

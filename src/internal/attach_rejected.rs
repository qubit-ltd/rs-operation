// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The error returned when a child control cannot be attached.

use crate::protocol::ManagedOperationControl;
use crate::report::AdmissionError;

/// Preserves the admission failure and the control whose attachment failed.
pub(crate) struct AttachRejected {
    /// Describes why the registry rejected the attachment.
    pub error: AdmissionError,
    /// Returns ownership of the control to the caller.
    pub control: Box<dyn ManagedOperationControl>,
}

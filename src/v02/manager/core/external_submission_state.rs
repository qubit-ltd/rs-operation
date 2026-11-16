// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Submission state shared by synchronous and asynchronous external drivers.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;

use super::ExternalExecutionContext;
use super::ExternalOutcomePublisher;
use crate::v02::adapter::ManagedOperationControl;
use crate::v02::protocol::OperationControl;

/// Optional external-driver state passed through manager submission paths.
///
/// # Type Parameters
///
/// - `T`: Successful outcome value produced by the external operation.
/// - `E`: Error outcome value produced by the external operation.
pub struct ExternalSubmissionState<T, E> {
    /// Optional protocol control installed on the accepted operation.
    pub(super) external_control: Option<Arc<dyn OperationControl>>,
    /// Whether the operation must confirm its input release during execution.
    pub(super) confirm_input_release: bool,
    /// Optional callback used when an accepted operation is abandoned.
    pub(super) on_abandon: Option<Box<dyn FnOnce() + Send>>,
    /// Slot for manager control installed before external work is dispatched.
    pub(super) external_managed: Option<Arc<Mutex<Option<Arc<ManagedOperationControl>>>>>,
    /// Slot for the external execution lifecycle state.
    pub(super) external_execution: Option<Arc<Mutex<Option<Arc<ExternalExecutionContext>>>>>,
    /// Optional one-shot publisher for a typed external outcome.
    pub(super) early_publisher: Option<ExternalOutcomePublisher<T, E>>,
    /// Optional gate controlling whether the external body may activate.
    pub(super) activation_permitted: Option<Arc<AtomicBool>>,
}

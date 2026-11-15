// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::operation_core::OperationCore;
use super::outcome_slot::OutcomeSlot;
use super::stop_control::StopControl;

/// Internal ownership parts shared by synchronous and asynchronous handles.
///
/// # Type Parameters
///
/// - `T`: Successful outcome value carried by the operation handle.
/// - `E`: Failure value carried by the operation handle.
pub(in crate::v02::operation) struct HandleParts<T, E> {
    /// Untyped lifecycle state observed by both handles.
    pub(in crate::v02::operation) core: Arc<OperationCore>,
    /// Unique typed outcome slot consumed by the receiver.
    pub(in crate::v02::operation) slot: Arc<OutcomeSlot<T, E>>,
    /// Control used to propagate accepted stop requests.
    pub(in crate::v02::operation) stop: Arc<dyn StopControl>,
}

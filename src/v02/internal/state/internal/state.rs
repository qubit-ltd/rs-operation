// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Outcome delivery and stop metadata coordinated under one state lock.

use super::ResultSlot;
use crate::v02::report::StopReport;

/// Mutable publication state shared by result observers and stop arbitration.
///
/// # Type Parameters
///
/// * `T` is the successful operation result type.
/// * `E` is the operation failure type.
pub(in crate::v02::internal::state) struct State<T, E> {
    /// One-shot slot holding the operation outcome or its delivery status.
    pub(in crate::v02::internal::state) result: ResultSlot<T, E>,
    /// Stop report recorded for the operation, if stop was requested.
    pub(in crate::v02::internal::state) stop: Option<StopReport>,
    /// Whether a successful outcome has been published to the result slot.
    pub(in crate::v02::internal::state) success_published: bool,
}

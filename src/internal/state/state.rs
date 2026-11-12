// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Facts serialized with the typed result and terminal settlement snapshot.

use std::sync::Arc;

use crate::report::SettlementReport;
use crate::report::StopReport;
use super::ResultSlot;

/// Stores the business result and independently observed lifecycle facts.
pub(super) struct State<T, E> {
    /// The one-shot typed result slot.
    pub(super) result: ResultSlot<T, E>,
    /// First accepted stop reason, independent of the business result.
    pub(super) stop: Option<StopReport>,
    /// Whether a successful outcome was published before any stop request.
    pub(super) success_published: bool,
    /// Immutable settlement report shared by all later observers.
    pub(super) settlement: Option<Arc<SettlementReport>>,
}

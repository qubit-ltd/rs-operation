// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::super::ManagedOperationControl;
use super::super::OperationCore;
use super::super::OutcomeSlot;
use super::super::ScopeCore;
use super::SubmissionLease;

/// State captured by an accepted runner until its executor job begins.
///
/// # Type Parameters
///
/// - `T`: The operation's successful output type.
/// - `E`: The operation's error type.
pub(crate) type RunnerParts<T, E> = (
    Arc<OperationCore>,
    Arc<OutcomeSlot<T, E>>,
    Arc<dyn SubmissionLease>,
    Option<Arc<ScopeCore>>,
    Arc<ManagedOperationControl>,
);

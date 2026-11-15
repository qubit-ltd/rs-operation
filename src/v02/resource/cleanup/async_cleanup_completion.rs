// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::AsyncCleanup;
use super::CleanupOutcome;
use super::UnconfirmedCleanup;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::report::CleanupEvidence;

/// Captures the result of polling one asynchronous cleanup policy.
///
/// Successful and failed outcomes are stored separately from panic recovery:
/// an unconfirmed completion keeps the registration and panic payload inside
/// `unconfirmed`, while retaining the policy until recovery takes ownership.
///
/// # Type Parameters
///
/// - `T`: The resource type returned by a failed cleanup outcome.
pub(crate) struct AsyncCleanupCompletion<T: Send + Sync + 'static> {
    /// The policy result, or `None` when polling panicked before confirmation.
    pub(crate) outcome: Option<CleanupOutcome<T>>,
    /// Evidence of a confirmed release; transfers and failures have no release
    /// evidence.
    pub(crate) evidence: Option<CleanupEvidence>,
    /// The policy owner retained for retry when the outcome failed.
    pub(crate) policy: Arc<dyn AsyncCleanup<T>>,
    /// Resource registration transferred to the completion handler on normal
    /// completion.
    pub(crate) registration: Option<ResourceRegistration>,
    /// Panic capsule retaining cleanup state when the result is unconfirmed.
    pub(crate) unconfirmed: Option<UnconfirmedCleanup>,
}

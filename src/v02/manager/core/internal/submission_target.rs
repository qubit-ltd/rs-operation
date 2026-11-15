// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Weak;

use super::super::OperationId;
use super::super::ScopeCore;

/// The immutable submission location carried by manager views.
#[derive(Clone)]
pub(crate) enum SubmissionTarget {
    /// Routes a root manager view into an optional operation group.
    Root {
        /// Group admission policy applied to root submissions, when present.
        group: Option<Arc<crate::v02::manager::group::GroupCore>>,
    },
    /// Routes submissions through a still-live parent operation scope.
    Parent {
        /// Stable identifier used to record the parent-child relationship.
        id: OperationId,
        /// Non-owning link that lets a child view detect a completed parent.
        scope: Weak<ScopeCore>,
        /// Group policy inherited by submissions through this parent view.
        group: Option<Arc<crate::v02::manager::group::GroupCore>>,
    },
}

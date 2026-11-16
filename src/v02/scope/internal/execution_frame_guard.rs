// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;

use super::ScopeCore;

/// Ends a scope when its operation body returns or unwinds, even if moved out.
#[must_use = "keep the execution frame guard alive until the operation body returns"]
pub(crate) struct ExecutionFrameGuard {
    /// Scope ended when this body frame returns or unwinds.
    scope: Arc<ScopeCore>,
}
impl ExecutionFrameGuard {
    /// Marks group execution started and binds scope end to this stack frame.
    ///
    /// # Parameters
    ///
    /// * `scope` — operation scope whose end is tied to this stack frame.
    pub(crate) fn new(scope: Arc<ScopeCore>) -> Self {
        for group in scope.groups() {
            group.execution_started(scope.id());
        }
        Self { scope }
    }
}
impl Drop for ExecutionFrameGuard {
    /// Ends the scope and dispatches resource cleanup.
    fn drop(&mut self) {
        self.scope.end();
    }
}

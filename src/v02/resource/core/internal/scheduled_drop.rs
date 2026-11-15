// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::Any;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;

use super::super::ScopeCompletion;
use super::panic_summary;
use crate::v02::manager::capacity::ResourceRegistration;
use crate::v02::report::CleanupEvidence;
use crate::v02::report::CleanupFailure;
use crate::v02::report::CleanupStage;
use crate::v02::report::DiagnosticSummary;
use crate::v02::report::ResourceId;
use crate::v02::resource::recovery::RecoveryHandle;

/// Owns a resource while its destructor runs on a cleanup worker.
///
/// Dropping this job catches destructor panics and settles the scope with
/// completion evidence or a failure and recovery handle. The resource
/// registration is released after a successful drop, or retained for recovery
/// when destruction panics.
///
/// # Type Parameters
///
/// - `T`: Resource value whose destructor runs on the cleanup worker.
pub(in crate::v02::resource::core) struct ScheduledDrop<T> {
    /// Value whose destructor is executed and monitored by this job.
    pub(in crate::v02::resource::core) value: Option<T>,
    /// Capacity registration released on success or held by panic recovery.
    pub(in crate::v02::resource::core) registration: Option<ResourceRegistration>,
    /// Resource identity attached to the settlement evidence or failure.
    pub(in crate::v02::resource::core) resource_id: ResourceId,
    /// Scope state receiving the destructor outcome and any recovery handle.
    pub(in crate::v02::resource::core) completion: Arc<ScopeCompletion>,
}

impl<T> Drop for ScheduledDrop<T> {
    /// Runs the resource destructor and records its outcome without unwinding.
    ///
    /// A destructor panic becomes cleanup failure evidence and an
    /// unconfirmed recovery handle; successful destruction records completion
    /// evidence and releases the registration.
    fn drop(&mut self) {
        let result = panic::catch_unwind(AssertUnwindSafe(|| drop(self.value.take())));
        let failure = result.as_ref().err().map(|payload| CleanupFailure {
            resource_id: self.resource_id,
            stage: CleanupStage::Drop,
            summary: DiagnosticSummary::new(&panic_summary(payload.as_ref())),
            source: None,
        });
        let evidence = result.is_ok().then_some(CleanupEvidence::DropCompleted);
        let recovery = result.err().map(|panic| {
            let unconfirmed =
                crate::v02::resource::cleanup::UnconfirmedCleanup::from_panic(self.registration.take(), panic);
            let payload = Arc::new(Mutex::new(Some(unconfirmed))) as Arc<dyn Any + Send + Sync>;
            let core = crate::v02::resource::recovery::RecoveryCore::new_unconfirmed(vec![self.resource_id], payload);
            RecoveryHandle::new(Arc::new(core))
        });
        if evidence.is_some() {
            drop(self.registration.take());
        }
        self.completion.complete(self.resource_id, evidence, failure, recovery);
    }
}

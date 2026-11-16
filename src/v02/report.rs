// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Version 0.2 operation identities, outcomes, limits, and reports.

mod child;
mod cleanup;
mod context;
mod diagnostics;
mod effect;
mod group;
mod identity;
mod internal;
mod manager;
mod operation;
mod options;
mod recovery;
mod resource;
mod settlement;
mod shutdown;

// Public types are re-exported here to preserve the v02::report API.
pub use child::ChildFailureSummary;
pub use child::ChildOutcomeKind;
pub use child::ChildOutcomeSummary;
pub use child::ChildRelease;
pub use child::ChildSettlement;
pub use child::ChildSnapshot;
pub use cleanup::CleanupEvidence;
pub use cleanup::CleanupFailure;
pub use cleanup::CleanupStage;
pub use cleanup::ResourceCleanupReport;
pub use context::ContextError;
pub use diagnostics::DiagnosticSummary;
pub use diagnostics::ExecutionFailure;
pub use diagnostics::PanicSummary;
pub use effect::EffectFact;
pub use effect::EffectRecord;
pub use effect::EffectRecordError;
pub use group::GroupSnapshot;
pub use group::GroupBindingError;
pub use identity::GroupId;
pub use identity::ManagerId;
pub use identity::OperationId;
pub use identity::RecoveryId;
pub use identity::RecoveryStoreId;
pub use identity::ResourceId;
pub use identity::ResourceScopeId;
pub use manager::ManagerCreateError;
pub use manager::ManagerSnapshot;
pub use operation::AdmissionError;
pub use operation::ExecutionPhase;
pub use operation::ObservationError;
pub use operation::OperationCompletion;
pub use operation::OperationOutcome;
pub use operation::OperationSnapshot;
pub use operation::OutcomeAvailability;
pub use operation::OutcomeError;
pub use operation::ResourceProgress;
pub use operation::RunError;
pub use operation::StopReason;
pub use operation::StopReport;
pub use operation::StopRequestStatus;
pub use operation::SubmitError;
pub use options::ChildFailurePolicy;
pub use options::ChildRequirement;
pub use options::ContextOptions;
pub use options::GroupOptions;
pub use options::ManagerLimits;
pub use options::ManagerOptions;
pub use options::OperationOptions;
pub use options::PortableExecutorOptions;
pub use options::RecoveryStoreOptions;
pub use options::ResourceScopeOptions;
pub use options::ScopeLimits;
pub use options::ServiceResourceOptions;
pub use options::ThreadPoolExecutorOptions;
pub use recovery::RecoveryAttemptReport;
pub use recovery::RecoveryPhase;
pub use recovery::RecoverySnapshot;
pub use recovery::RecoveryStoreCreateError;
pub use recovery::RecoveryStoreSnapshot;
pub use recovery::RecoveryTransfer;
pub use recovery::RecoveryTransferError;
pub use recovery::RecoveryTransferReceipt;
pub use recovery::RetryError;
pub use resource::CleanupDependencyError;
pub use resource::ResourceCapacityError;
pub use resource::ResourceLookupError;
pub use resource::ResourceManageErrorKind;
pub use resource::ResourceScopeError;
pub use resource::ResourceScopeReport;
pub use resource::ResourceScopeSnapshot;
pub use resource::ResourceTransfer;
pub use resource::ResourceTransferError;
pub use resource::TransferReceipt;
pub use settlement::SettlementReport;
pub use settlement::SettlementStatus;
pub use settlement::SettlementWait;
pub use shutdown::ShutdownMode;
pub use shutdown::ShutdownReport;
pub use shutdown::ShutdownSnapshot;
pub use shutdown::ShutdownWait;

pub use crate::v02::resource::RecoveryHandle;

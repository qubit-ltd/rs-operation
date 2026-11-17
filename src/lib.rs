// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! # Qubit Operation
//!
//! Typed operation lifecycles with bounded admission, scoped resources, and
//! separate outcome and settlement reporting.

mod v02;

#[cfg(test)]
mod tests;

pub use v02::adapter::ExternalBindError;
pub use v02::adapter::PreparedExternalOperation;
pub use v02::context::CleanupContext;
pub use v02::context::DeadlineSignal;
pub use v02::context::OperationContext;
pub use v02::context::StopSignal;
pub use v02::manager::core::AsyncOperationManager;
pub use v02::manager::core::OperationManager;
pub use v02::manager::group::AsyncOperationGroup;
pub use v02::manager::group::OperationGroup;
pub use v02::manager::shutdown::AsyncShutdownHandle;
pub use v02::manager::shutdown::ShutdownHandle;
pub use v02::operation::AsyncOperationHandle;
pub use v02::operation::AsyncOperationObserver;
pub use v02::operation::OperationHandle;
pub use v02::operation::OperationObserver;
pub use v02::protocol::BudgetError;
pub use v02::protocol::BudgetPolicy;
pub use v02::protocol::ClockDomainId;
pub use v02::protocol::ClockDomainMismatch;
pub use v02::protocol::ClockSource;
pub use v02::protocol::Deadline;
pub use v02::protocol::MonotonicTime;
pub use v02::protocol::OperationControl;
pub use v02::protocol::TimerRegistration;
pub use v02::report::AdmissionError;
pub use v02::report::ChildFailurePolicy;
pub use v02::report::ChildFailureSummary;
pub use v02::report::ChildOutcomeKind;
pub use v02::report::ChildOutcomeSummary;
pub use v02::report::ChildRelease;
pub use v02::report::ChildRequirement;
pub use v02::report::ChildSettlement;
pub use v02::report::ChildSnapshot;
pub use v02::report::CleanupDependencyError;
pub use v02::report::CleanupEvidence;
pub use v02::report::CleanupFailure;
pub use v02::report::CleanupStage;
pub use v02::report::ContextError;
pub use v02::report::ContextOptions;
pub use v02::report::DiagnosticSummary;
pub use v02::report::EffectFact;
pub use v02::report::EffectRecord;
pub use v02::report::EffectRecordError;
pub use v02::report::ExecutionFailure;
pub use v02::report::ExternalProtocolFault;
pub use v02::report::ExternalProtocolFaultKind;
pub use v02::report::ExternalProtocolPort;
pub use v02::report::ExecutionPhase;
pub use v02::report::GroupBindingError;
pub use v02::report::GroupId;
pub use v02::report::GroupOptions;
pub use v02::report::GroupSnapshot;
pub use v02::report::ManagerCreateError;
pub use v02::report::ManagerId;
pub use v02::report::ManagerLimits;
pub use v02::report::ManagerOptions;
pub use v02::report::ManagerSnapshot;
pub use v02::report::ObservationError;
pub use v02::report::OperationCompletion;
pub use v02::report::OperationId;
pub use v02::report::OperationOptions;
pub use v02::report::OperationOutcome;
pub use v02::report::OperationSnapshot;
pub use v02::report::OutcomeAvailability;
pub use v02::report::OutcomeError;
pub use v02::report::PanicSummary;
pub use v02::report::PortableExecutorOptions;
pub use v02::report::RecoveryAttemptReport;
pub use v02::report::RecoveryId;
pub use v02::report::RecoveryPhase;
pub use v02::report::RecoverySnapshot;
pub use v02::report::RecoveryStoreCreateError;
pub use v02::report::RecoveryStoreId;
pub use v02::report::RecoveryStoreSnapshot;
pub use v02::report::RecoveryTransfer;
pub use v02::report::RecoveryTransferError;
pub use v02::report::RecoveryTransferReceipt;
pub use v02::report::ResourceCleanupReport;
pub use v02::report::ResourceId;
pub use v02::report::ResourceLookupError;
pub use v02::report::ResourceManageErrorKind;
pub use v02::report::ResourceProgress;
pub use v02::report::ResourceScopeError;
pub use v02::report::ResourceScopeId;
pub use v02::report::ResourceScopeOptions;
pub use v02::report::ResourceScopeReport;
pub use v02::report::ResourceScopeSnapshot;
pub use v02::report::ResourceTransfer;
pub use v02::report::ResourceTransferError;
pub use v02::report::RetryError;
pub use v02::report::RunError;
pub use v02::report::ScopeLimits;
pub use v02::report::ServiceResourceOptions;
pub use v02::report::SettlementReport;
pub use v02::report::SettlementStatus;
pub use v02::report::SettlementWait;
pub use v02::report::ShutdownMode;
pub use v02::report::ShutdownReport;
pub use v02::report::ShutdownSnapshot;
pub use v02::report::ShutdownWait;
pub use v02::report::StopReason;
pub use v02::report::StopReport;
pub use v02::report::StopRequestStatus;
pub use v02::report::SubmitError;
pub use v02::report::ThreadPoolExecutorOptions;
pub use v02::report::TransferReceipt;
pub use v02::resource::AsyncCleanup;
pub use v02::resource::AsyncRecoveryAttempt;
pub use v02::resource::CleanupFuture;
pub use v02::resource::CleanupOutcome;
pub use v02::resource::ManageResourceError;
pub use v02::resource::ManageWithError;
pub use v02::resource::RecoveryAttempt;
pub use v02::resource::RecoveryHandle;
pub use v02::resource::RecoveryStore;
pub use v02::resource::RecoveryStoreOptions;
pub use v02::resource::RecoveryStoreShutdownError;
pub use v02::resource::RecoveryStoreShutdownHandle;
pub use v02::resource::ResourceCapacityError;
pub use v02::resource::ResourceCommitError;
pub use v02::resource::ResourceCommitWithError;
pub use v02::resource::ResourceKey;
pub use v02::resource::ResourceRef;
pub use v02::resource::ResourceReservation;
pub use v02::resource::ResourceScope;
pub use v02::resource::ServiceResourceScope;
pub use v02::scope::OperationScope;

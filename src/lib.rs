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
pub use v02::report::*;
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

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

pub use v02::context::{CleanupContext, DeadlineSignal, OperationContext, StopSignal};
pub use v02::manager::core::{AsyncOperationManager, OperationManager};
pub use v02::manager::group::{AsyncOperationGroup, OperationGroup};
pub use v02::manager::shutdown::{AsyncShutdownHandle, ShutdownHandle};
pub use v02::operation::{
    AsyncOperationHandle, AsyncOperationObserver, OperationHandle, OperationObserver,
};
pub use v02::protocol::{
    BudgetError, BudgetPolicy, ClockDomainId, ClockDomainMismatch, ClockSource, Deadline,
    MonotonicTime, OperationControl, TimerRegistration,
};
pub use v02::report::*;
pub use v02::resource::{
    AsyncCleanup, AsyncRecoveryAttempt, CleanupFuture, CleanupOutcome, ManageResourceError,
    ManageWithError, RecoveryAttempt, RecoveryHandle, RecoveryStore, RecoveryStoreOptions,
    RecoveryStoreShutdownError, RecoveryStoreShutdownHandle, ResourceCapacityError,
    ResourceCommitError, ResourceCommitWithError, ResourceKey, ResourceRef, ResourceReservation,
    ResourceScope, ServiceResourceScope,
};
pub use v02::scope::OperationScope;

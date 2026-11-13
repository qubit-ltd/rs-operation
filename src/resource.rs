// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Bounded resource ownership, use leases, and cleanup dependencies.

mod cleanup_scheduler;
mod dependency_error;
mod internal;
mod ready_cleanup;
mod recovery_error;
mod recovery_registry;
mod register_error;
mod register_error_kind;
mod registry_error;
mod resource_error;
mod resource_handle;
mod resource_registry;
mod resource_use;
mod transfer_error;

pub use cleanup_scheduler::CleanupObservation;
pub use cleanup_scheduler::CleanupPanic;
pub use cleanup_scheduler::CleanupPermit;
pub use cleanup_scheduler::CleanupResponsibility;
pub use cleanup_scheduler::CleanupScheduler;
pub use cleanup_scheduler::CleanupTask;
pub use dependency_error::DependencyError;
pub use ready_cleanup::ReadyCleanup;
pub use recovery_error::RecoveryError;
pub use recovery_registry::RecoveryRegistry;
pub use register_error::RegisterError;
pub use register_error_kind::RegisterErrorKind;
pub use registry_error::RegistryError;
pub use resource_error::ResourceError;
pub use resource_handle::ResourceHandle;
pub use resource_registry::ResourceRegistry;
pub use resource_use::ResourceUse;
pub use transfer_error::TransferError;

pub use crate::report::ReleaseReceipt;

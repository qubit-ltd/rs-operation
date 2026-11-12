// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

//! Bounded resource ownership, use leases, and cleanup dependencies.

mod cleanup_scheduler;
mod dependency_error;
mod handle;
mod internal;
mod ready_cleanup;
mod recovery_registry;
mod register_error;
mod register_error_kind;
mod registry;
mod registry_error;
mod resource_error;
mod resource_use;
mod transfer_error;

pub use crate::report::ReleaseReceipt;
pub use cleanup_scheduler::{
    CleanupObservation, CleanupPanic, CleanupPermit, CleanupScheduler, CleanupTask,
};
pub use dependency_error::DependencyError;
pub use handle::ResourceHandle;
pub use ready_cleanup::ReadyCleanup;
pub use recovery_registry::{RecoveryError, RecoveryRegistry};
pub use register_error::RegisterError;
pub use register_error_kind::RegisterErrorKind;
pub use registry::ResourceRegistry;
pub use registry_error::RegistryError;
pub use resource_error::ResourceError;
pub use resource_use::ResourceUse;
pub use transfer_error::TransferError;

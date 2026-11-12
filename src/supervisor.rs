// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Bounded root admission for managed operations.
//!
//! This module owns admission bookkeeping only. Operation execution and result
//! observation are attached by the operation handle layer.

use crate::context::ContextError;

mod admission_wait;
mod cleanup_permit;
mod internal;
mod operation_supervisor;
mod service_group;
mod service_operation_handle;
mod service_request;
mod service_resource_error;
mod service_resource_scope;
mod shutdown_mode;
mod shutdown_observation;
mod shutdown_observer;
mod shutdown_report;
mod root_admission;
mod shutdown_wait_registration;

pub use admission_wait::AdmissionWait;
pub use cleanup_permit::CleanupPermit;
pub use operation_supervisor::OperationSupervisor;
pub use root_admission::RootAdmission;
pub use service_group::ServiceGroup;
pub use service_operation_handle::ServiceOperationHandle;
pub use service_request::ServiceRequest;
pub use service_resource_error::ServiceResourceError;
pub use service_resource_scope::ServiceResourceScope;
pub use shutdown_mode::ShutdownMode;
pub use shutdown_observation::ShutdownObservation;
pub use shutdown_observer::ShutdownObserver;
pub use shutdown_report::ShutdownReport;
pub(crate) use internal::AdmittedTask;
pub(crate) use internal::ResultLease;
pub(crate) use internal::TaskLease;
use internal::SharedServiceGroupState;
use internal::ShutdownSource;
use internal::util::lock;

#[cfg(test)]
#[path = "supervisor/tests.rs"]
mod tests;

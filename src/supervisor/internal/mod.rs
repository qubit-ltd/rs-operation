// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Private supervisor state and lifecycle helpers.

pub(super) mod admitted_task;
pub(super) mod inner;
pub(super) mod lease_state;
pub(super) mod leases;
pub(super) mod result_lease;
pub(super) mod service_group_lifecycle;
pub(super) mod service_group_state;
pub(super) mod service_request_entry;
pub(super) mod shared_service_group_state;
pub(super) mod shutdown_source;
pub(super) mod state;
pub(super) mod task_lease;
pub(super) mod util;

pub(super) use admitted_task::AdmittedTask;
pub(super) use inner::Inner;
pub(super) use lease_state::LeaseState;
pub(super) use result_lease::ResultLease;
pub(super) use service_group_state::ServiceGroupState;
pub(super) use service_request_entry::ServiceRequestEntry;
pub(super) use shared_service_group_state::SharedServiceGroupState;
pub(super) use shutdown_source::ShutdownSource;
pub(super) use state::State;
pub(super) use task_lease::TaskLease;

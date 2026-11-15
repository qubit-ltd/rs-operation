// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Shared time and budget protocols for operation contexts.

mod budget_error;
mod budget_policy;
mod clock_domain_id;
mod clock_domain_mismatch;
mod clock_source;
mod deadline;
mod monotonic_time;
mod operation_control;
mod timer_registration;

pub use budget_error::BudgetError;
pub use budget_policy::BudgetPolicy;
pub use clock_domain_id::ClockDomainId;
pub use clock_domain_mismatch::ClockDomainMismatch;
pub use clock_source::ClockSource;
pub use deadline::Deadline;
pub use monotonic_time::MonotonicTime;
pub use operation_control::OperationControl;
pub use timer_registration::TimerRegistration;

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Child registration and structured scope composition.

mod child_requirement;
mod child_reservation;
mod internal;
mod join_facts;
mod operation_scope;
mod scope_error;
mod scope_join_report;
mod scope_options;
mod scope_policy;

pub use child_requirement::ChildRequirement;
pub use child_reservation::ChildReservation;
pub(crate) use internal::poll::poll_scope_business;
pub(crate) use internal::poll::poll_scope_releases;
pub(crate) use join_facts::JoinFacts;
pub use operation_scope::OperationScope;
pub use scope_error::ScopeError;
pub use scope_join_report::ScopeJoinReport;
pub use scope_options::ScopeOptions;
pub use scope_policy::ScopePolicy;

#[cfg(test)]
mod tests;

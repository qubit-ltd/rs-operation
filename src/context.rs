// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Cancellation, deadline, and budget references shared by related operations.

// Caller-owned budget policy contract.
mod budget_lease;
// Error returned when a caller-owned budget policy rejects an operation.
mod budget_lease_error;
// Context creation and admission errors.
mod context_error;
// Options inherited when creating root and child contexts.
mod context_options;
// Clock-domain deadlines and their signaling future.
mod deadline;
// Private cancellation and budget implementation types.
mod internal;
// Shared operation context and its public lifecycle methods.
mod operation_context;
// Future that observes the first accepted stop request.
mod stop_signal;

pub use budget_lease::BudgetLease;
pub use budget_lease_error::BudgetLeaseError;
pub use context_error::ContextError;
pub use context_options::ContextOptions;
pub use deadline::ClockDomainMismatch;
pub use deadline::Deadline;
pub use deadline::DeadlineSignal;
pub use operation_context::OperationContext;
pub use stop_signal::StopSignal;

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Absolute deadlines and their clock-backed timer future.

// Error returned when instants belong to different clock domains.
mod clock_domain_mismatch;
// Absolute deadline value and domain-aware comparisons.
mod deadline;
// Future that registers and cancels a deadline timer.
mod deadline_signal;

pub use clock_domain_mismatch::ClockDomainMismatch;
pub use deadline::Deadline;
pub use deadline_signal::DeadlineSignal;

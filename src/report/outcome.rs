// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Typed business outcomes, stop reports, and side-effect facts.

mod child_business_outcome;
mod child_business_summary;
mod child_failure_summary;
mod effect_fact;
mod effect_record;
mod operation_outcome;
mod panic_summary;
mod stop_reason;
mod stop_report;

pub use child_business_outcome::ChildBusinessOutcome;
pub use child_business_summary::ChildBusinessSummary;
pub use child_failure_summary::ChildFailureSummary;
pub use effect_fact::EffectFact;
pub use effect_record::EffectRecord;
pub use operation_outcome::OperationOutcome;
pub use panic_summary::PanicSummary;
pub use stop_reason::CancelReason;
pub use stop_reason::StopReason;
pub use stop_report::StopReport;

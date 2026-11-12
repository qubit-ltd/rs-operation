// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Typed failures returned by report-related operation APIs.

mod admission_error;
mod observation_error;
mod start_error;

pub use admission_error::AdmissionError;
pub use observation_error::ObservationError;
pub use start_error::StartError;

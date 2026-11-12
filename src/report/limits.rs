// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Finite capacities for operation scopes and supervisors.

mod scope_limits;
mod supervisor_limits;

pub use scope_limits::ScopeLimits;
pub use supervisor_limits::SupervisorLimits;

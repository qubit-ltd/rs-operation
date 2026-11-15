// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::protocol::ClockSource;
use crate::v02::protocol::Deadline;
use crate::v02::report::ContextError;

/// Validates that a configured deadline has a clock in the same domain.
///
/// # Parameters
///
/// * `clock` is the clock used to measure the deadline, if configured.
/// * `deadline` is the optional deadline whose clock domain is checked.
pub(in crate::v02::context) fn validate_clock(
    clock: Option<&dyn ClockSource>,
    deadline: Option<Deadline>,
) -> Result<(), ContextError> {
    if let Some(deadline) = deadline {
        let Some(clock) = clock else {
            return Err(ContextError::ClockUnavailable);
        };
        if clock.now().domain() != deadline.time_point().domain() {
            return Err(ContextError::ClockDomainMismatch);
        }
    }
    Ok(())
}

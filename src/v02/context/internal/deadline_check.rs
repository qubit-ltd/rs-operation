// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use crate::v02::protocol::ClockSource;
use crate::v02::protocol::Deadline;

/// Reports whether a deadline has elapsed or belongs to a different clock.
///
/// # Parameters
///
/// * `clock` measures the current time.
/// * `deadline` is the deadline to compare against that clock.
pub(in crate::v02::context) fn deadline_reached(clock: &dyn ClockSource, deadline: Deadline) -> bool {
    let now = clock.now();
    now.domain() != deadline.time_point().domain() || now.ticks() >= deadline.time_point().ticks()
}

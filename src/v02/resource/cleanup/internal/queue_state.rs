// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::VecDeque;

use crate::v02::resource::cleanup_port::CleanupJob;

/// Admission and FIFO state for one cleanup dispatcher.
pub(in crate::v02::resource::cleanup) struct QueueState {
    /// Whether new reservations may be created; existing permits remain usable.
    pub(in crate::v02::resource::cleanup) accepting: bool,
    /// Reservations include uncommitted resources and jobs still pending in
    /// this queue. Each registered resource owns at most one reservation.
    pub(in crate::v02::resource::cleanup) reserved: usize,
    /// Cleanup jobs awaiting executor capacity, in enqueue order.
    pub(in crate::v02::resource::cleanup) pending: VecDeque<Box<dyn CleanupJob>>,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

// Stores shared scheduler counters, waiters, and task ownership.
mod scheduler_state;
// Stores synchronization-protected state shared by cleanup task observers.
mod cleanup_task_state;
// Stores mutable future lifecycle data while it is polled.
mod cleanup_task_inner;

pub(super) use self::cleanup_task_inner::CleanupTaskInner;
pub(super) use self::cleanup_task_state::CleanupTaskState;
pub(super) use self::scheduler_state::SchedulerState;

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Object-safe ports used to manage domain-owned operations and cleanup.

mod cleanup;
mod managed_operation_control;
mod time;

pub use self::cleanup::CleanupContext;
pub use self::cleanup::OwnedCleanup;
pub use self::managed_operation_control::ManagedOperationControl;
pub use self::time::ClockSource;
pub use self::time::TimerRegistration;
pub use crate::report::StopReason;

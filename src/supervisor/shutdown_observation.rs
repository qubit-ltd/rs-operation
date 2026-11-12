// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shutdown report paired with an observer that can wait again.

use super::ShutdownObserver;

/// A shutdown snapshot and an observer that can wait again after a timeout.
#[derive(Clone)]
pub struct ShutdownObservation {
    pub report: ShutdownReport,
    pub observer: ShutdownObserver,
}


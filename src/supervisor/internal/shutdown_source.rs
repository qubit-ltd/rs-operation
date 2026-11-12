// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Source state used by reusable shutdown observers.

use super::SharedServiceGroupState;
use crate::supervisor::OperationSupervisor;

#[derive(Clone)]
pub(super) enum ShutdownSource {
    Supervisor(OperationSupervisor),
    Service(SharedServiceGroupState, OperationSupervisor),
}


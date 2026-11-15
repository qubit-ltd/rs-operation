// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::Any;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::OnceLock;

use crate::v02::report::OperationId;

/// Type-erased resource references retained after an external runtime is lost.
pub(in crate::v02::adapter) type LostInputs = HashMap<OperationId, Box<dyn Any + Send + Sync>>;
/// Process-wide retained inputs awaiting independent release evidence.
pub(in crate::v02::adapter) static LOST_INPUTS: OnceLock<Mutex<LostInputs>> = OnceLock::new();

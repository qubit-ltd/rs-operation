// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use super::RegistryState;

use crate::report::OperationId;

use std::sync::Mutex;

pub(in crate::resource) struct RegistryInner {
    pub(in crate::resource) id: u64,
    pub(in crate::resource) operation_id: OperationId,
    pub(in crate::resource) state: Mutex<RegistryState>,
}

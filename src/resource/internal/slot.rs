// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use std::sync::Arc;

use super::Entry;

pub(in crate::resource) struct Slot {
    pub(in crate::resource) generation: u32,
    pub(in crate::resource) exhausted: bool,
    pub(in crate::resource) entry: Option<Arc<Entry>>,
}

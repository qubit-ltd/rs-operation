// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use super::EntryPhase;

use crate::report::ResourceId;

pub(in crate::resource) struct EntryState {
    pub(in crate::resource) id: ResourceId,
    pub(in crate::resource) phase: EntryPhase,
    pub(in crate::resource) active_uses: usize,
    pub(in crate::resource) transfer_source: Option<ResourceId>,
    pub(in crate::resource) transfer_receiver: Option<ResourceId>,
}

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

use super::EntryState;
use super::RegistryInner;

use crate::protocol::OwnedCleanup;

use std::sync::Mutex;
use std::sync::Weak;

pub(in crate::resource) struct Entry {
    pub(in crate::resource) registry: Mutex<Weak<RegistryInner>>,
    pub(in crate::resource) state: Mutex<EntryState>,
    pub(in crate::resource) owner: Mutex<Option<Box<dyn OwnedCleanup>>>,
}

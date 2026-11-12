// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License").
// =============================================================================

#[derive(Clone, Copy, Eq, PartialEq)]
pub(in crate::resource) enum EntryPhase {
    Open,
    Closed,
    Cleaning,
    Transferred,
    Released,
}

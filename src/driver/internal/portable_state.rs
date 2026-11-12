// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;

use super::PortableTask;

/// Task registry and ready queue protected by the portable scheduler mutex.
#[derive(Default)]
pub struct PortableState {
    pub tasks: HashMap<u64, Arc<PortableTask>>,
    pub ready: VecDeque<u64>,
    pub next_id: u64,
    pub closed: bool,
}

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

/// Erased owned job stored in the bounded thread executor queue.
pub type ThreadJob = Box<dyn FnOnce() + Send + 'static>;

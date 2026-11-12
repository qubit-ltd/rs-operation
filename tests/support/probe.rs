// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

/// Owned marker whose actual destruction increments a shared counter.
pub struct DropProbe {
    count: Arc<AtomicUsize>,
}

/// Read-only handle for observing destruction of [`DropProbe`].
#[derive(Clone)]
pub struct DropProbeCounter {
    count: Arc<AtomicUsize>,
}

impl DropProbe {
    /// Create a probe and a counter that observes its destruction.
    pub fn new() -> (Self, DropProbeCounter) {
        let count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                count: Arc::clone(&count),
            },
            DropProbeCounter { count },
        )
    }
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

impl DropProbeCounter {
    /// Return the number of probe values that have actually been dropped.
    pub fn drop_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

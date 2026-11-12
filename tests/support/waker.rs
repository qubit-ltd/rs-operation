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
use std::task::Wake;
use std::task::Waker;

/// Counts actual calls to a task waker.
#[derive(Clone, Default)]
pub struct CountingWaker {
    count: Arc<AtomicUsize>,
}

impl CountingWaker {
    /// Create a counter whose value starts at zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a waker that increments this counter when woken.
    pub fn waker(&self) -> Waker {
        Waker::from(Arc::new(CountWake(Arc::clone(&self.count))))
    }

    /// Return the number of calls made through wakers from this counter.
    pub fn wake_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

struct CountWake(Arc<AtomicUsize>);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

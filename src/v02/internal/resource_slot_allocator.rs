// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Process-wide allocator for opaque resource slot identities.

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

static NEXT_RESOURCE_SLOT: AtomicU64 = AtomicU64::new(1);

/// Allocates a process-unique slot, returning `None` after the integer space is
/// exhausted.
pub(crate) fn allocate() -> Option<u64> {
    allocate_from(&NEXT_RESOURCE_SLOT)
}

/// Allocates from a supplied counter for production and boundary tests.
///
/// # Parameters
///
/// * `counter` — atomic slot counter whose value advances without wrapping.
///
/// # Returns
///
/// The previous counter value, or `None` when incrementing would overflow.
fn allocate_from(counter: &AtomicU64) -> Option<u64> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| value.checked_add(1))
        .ok()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU64;
    use std::thread;

    use super::allocate_from;

    #[test]
    fn test_allocation_exhaustion_does_not_wrap_or_recover() {
        let counter = AtomicU64::new(u64::MAX - 1);
        assert_eq!(allocate_from(&counter), Some(u64::MAX - 1));
        assert_eq!(allocate_from(&counter), None);
        assert_eq!(allocate_from(&counter), None);
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn test_concurrent_allocations_are_unique() {
        let counter = Arc::new(AtomicU64::new(1));
        let allocated = Arc::new(Mutex::new(Vec::new()));
        let workers = (0..8)
            .map(|_| {
                let counter = Arc::clone(&counter);
                let allocated = Arc::clone(&allocated);
                thread::spawn(move || {
                    let local = (0..128)
                        .map(|_| allocate_from(&counter).expect("counter is not exhausted"))
                        .collect::<Vec<_>>();
                    allocated.lock().unwrap().extend(local);
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
        let allocated = allocated.lock().unwrap();
        let unique = allocated.iter().copied().collect::<HashSet<_>>();
        assert_eq!(allocated.len(), 8 * 128);
        assert_eq!(unique.len(), allocated.len());
    }
}

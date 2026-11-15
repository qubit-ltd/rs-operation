// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::sync::Arc;

use super::thread_wake::ThreadWake;

/// Polls a future on the current thread until it produces an output.
///
/// When the future is pending, the current thread parks until its waker
/// unparks it; this helper does not use a separate executor.
///
/// # Type Parameters
///
/// * `F` — future whose output the caller needs synchronously.
///
/// # Parameters
///
/// * `future` — future to poll to completion.
///
/// # Returns
///
/// The output produced by `future`.
///
/// # Panics
///
/// Panics if polling `future` panics; the panic propagates to the caller.
pub(in crate::v02::resource::recovery) fn block_on<F: Future>(future: F) -> F::Output {
    let waker = std::task::Waker::from(Arc::new(ThreadWake(std::thread::current())));
    let mut context = std::task::Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        std::thread::park();
    }
}

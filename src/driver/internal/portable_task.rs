// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::task::Waker;

/// A heap-allocated, movable future that completes without producing a value.
///
/// The `Send` bound allows the driver to move the future between worker
/// threads, and the `'static` bound lets it retain the future independently of
/// the task's creator.
pub type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Stored future and coalesced ready state for one registered task.
pub struct PortableTask {
    /// Holds the task's future until it completes; the mutex permits taking
    /// and polling the future without holding the task registry lock.
    pub future: Mutex<Option<BoxFuture>>,
    /// Indicates whether a wakeup for this task is already queued, coalescing
    /// repeated wakeups into a single ready-queue entry.
    pub queued: AtomicBool,
    /// Wakes the driver when the task becomes ready to be polled again.
    pub wake: Waker,
}

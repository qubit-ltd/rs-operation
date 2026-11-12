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

pub type BoxFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

/// Stored future and coalesced ready state for one registered task.
pub struct PortableTask {
    pub future: Mutex<Option<BoxFuture>>,
    pub queued: AtomicBool,
    pub wake: Waker,
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! The boxed future type used by internal execution ports.

use core::future::Future;
use core::pin::Pin;

/// An owned, sendable future whose output type is selected by the caller.
///
/// Boxing keeps internal port signatures independent of each submitted
/// future's concrete type and permits the future to move between threads.
///
/// # Type Parameters
///
/// * `T` - The value produced when the future completes.
pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;

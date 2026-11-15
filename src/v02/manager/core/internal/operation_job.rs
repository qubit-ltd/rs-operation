// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;

/// A sync closure or async future whose frame guard ends only on
/// completion/drop.
pub(crate) enum OperationJob {
    /// A one-shot synchronous operation body.
    Sync(
        /// Synchronous operation closure run by the blocking executor.
        Box<dyn FnOnce() + Send + 'static>,
    ),
    /// A pinned async operation body whose frame guard follows its lifetime.
    Async(
        /// Async operation future polled by the portable executor.
        Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ),
}

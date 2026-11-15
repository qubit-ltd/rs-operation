// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use super::lock;

/// Invokes the registered callback after releasing its mutex.
///
/// Cloning the callback under the lock and invoking it afterward allows
/// callback code to safely re-enter executor capacity paths.
///
/// # Parameters
///
/// - `callback`: Optional capacity-change callback protected by its mutex.
pub(in crate::v02::executor) fn notify_capacity(callback: &Mutex<Option<Arc<dyn Fn() + Send + Sync + 'static>>>) {
    let callback = lock(callback).as_ref().map(Arc::clone);
    if let Some(callback) = callback {
        callback();
    }
}

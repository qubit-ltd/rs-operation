// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::pin::Pin;

use crate::v02::report::CleanupFailure;

/// Sendable future returned by one recovery action.
///
/// `Ok(())` confirms the action resolved cleanup. `Err` carries the cleanup
/// failure while responsibility for the resource remains unresolved.
pub(crate) type RecoveryFuture = Pin<Box<dyn Future<Output = Result<(), CleanupFailure>> + Send + 'static>>;

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared state for coordinating synchronous body completion.

use core::task::Waker;

use super::BlockingBodyResult;

/// Coordinates synchronous body completion with the managed async supervisor.
///
/// # Type Parameters
///
/// * `T` is the success value returned by the synchronous operation body.
/// * `E` is the application error returned by the synchronous operation body.
pub(crate) struct BlockingCompletion<T, E> {
    /// Result supplied by the executor, if it completed.
    pub(crate) result: Option<BlockingBodyResult<T, E>>,
    /// Whether executor submission failed before a result could arrive.
    pub(crate) aborted: bool,
    /// Supervisor waker waiting for body completion.
    pub(crate) waker: Option<Waker>,
}

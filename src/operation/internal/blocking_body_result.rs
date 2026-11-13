// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Result states produced by synchronous operation bodies.

/// Outcome produced by the caller-supplied synchronous operation body.
///
/// # Type Parameters
///
/// - `T`: The successful value returned by the body.
/// - `E`: The application error returned by the body.
pub(crate) enum BlockingBodyResult<T, E> {
    /// The body returned its business result.
    Returned(
        /// Business success or application error returned by the body.
        Result<T, E>,
    ),
    /// The body panicked while being caught at the executor boundary.
    Panicked,
}

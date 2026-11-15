// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Explicit global manager shutdown and observation handles.

mod async_shutdown_handle;
mod internal;
mod shutdown_handle;

pub use async_shutdown_handle::AsyncShutdownHandle;
pub use shutdown_handle::ShutdownHandle;

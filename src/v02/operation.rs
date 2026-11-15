// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Typed, single-consumer outcomes and untyped lifecycle observation.

mod async_operation_handle;
mod async_operation_observer;
mod internal;
mod operation_handle;
mod operation_observer;

pub use async_operation_handle::AsyncOperationHandle;
pub use async_operation_observer::AsyncOperationObserver;
pub(crate) use internal::OperationCore;
pub(crate) use internal::OutcomeDelivery;
pub(crate) use internal::OutcomeSlot;
pub(crate) use internal::StopControl;
pub use operation_handle::OperationHandle;
pub use operation_observer::OperationObserver;

#[cfg(test)]
mod tests;

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Admission and lifecycle ports for externally executed child operations.

mod external_operation;

mod internal;

#[cfg(test)]
mod tests;

pub use external_operation::ExternalBindError;
pub use external_operation::PreparedExternalOperation;
pub(crate) use internal::AdapterErrorKind;
pub(crate) use internal::AdapterReservation;
#[cfg(test)]
pub(crate) use internal::ExternalTask;
pub(crate) use internal::ManagedOperationControl;
#[cfg(test)]
pub(crate) use internal::recover_lost_input;

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Atomic movement of complete resource dependency groups.
#![allow(dead_code)] // Public scope and RecoveryStore wiring is completed by T11.

mod internal;
mod transfer_registry;
#[cfg(test)]
pub(crate) use transfer_registry::TransferRegistry;

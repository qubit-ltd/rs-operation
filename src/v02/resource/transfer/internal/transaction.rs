// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Mutex;

/// Serializes transfer validation and ownership moves across all registries.
///
/// A transfer holds this lock while checking source/target entries and
/// dependency-group state, then moving the group as one transaction.
pub(in crate::v02::resource::transfer) static TRANSFER_TRANSACTION: Mutex<()> = Mutex::new(());

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::any::Any;

/// Converts a resource destructor's panic payload into diagnostic text.
///
/// String and static string payloads retain their message; other payloads use
/// a generic description because their concrete type has no display contract.
///
/// # Parameters
///
/// - `payload`: Panic payload captured from a resource destructor.
///
/// # Returns
///
/// A diagnostic summary that can be stored in cleanup failure evidence.
#[must_use = "the panic summary should be included in cleanup diagnostics"]
pub(in crate::v02::resource::core) fn panic_summary(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        format!("resource destructor panicked: {message}")
    } else if let Some(message) = payload.downcast_ref::<&'static str>() {
        format!("resource destructor panicked: {message}")
    } else {
        "resource destructor panicked with a non-string payload".to_owned()
    }
}

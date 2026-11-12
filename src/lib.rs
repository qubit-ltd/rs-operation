// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! # Qubit Operation
//!
//! Foundation for managed operation lifecycles and composition.
//!
//! The crate currently exposes lifecycle protocol and report types. Managed
//! operation execution and composition are being built on these foundations.

mod internal;

pub mod protocol;
pub mod report;

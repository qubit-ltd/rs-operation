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
//! The crate exposes lifecycle contracts, portable drivers, and resource
//! registry primitives used to compose managed operations.

mod internal;

pub mod context;
pub mod driver;
pub mod protocol;
pub mod report;
pub mod resource;

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
//! The crate exposes lifecycle contracts, portable drivers, resource registry
//! primitives, and bounded admission for managed operations.

mod internal;

pub mod context;
pub mod driver;
pub mod operation;
pub mod protocol;
pub mod report;
pub mod resource;
pub mod scope;
pub mod supervisor;

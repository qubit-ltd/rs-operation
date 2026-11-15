// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// EXPECT: Clone
use qubit_operation::OperationScope;
fn requires_clone<T: Clone>() {}
fn check() {
    requires_clone::<OperationScope>();
}
fn main() {}

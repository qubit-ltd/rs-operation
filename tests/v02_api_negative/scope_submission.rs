// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// EXPECT: try_submit
use qubit_operation::OperationScope;
fn check(scope: &OperationScope) {
    let _ = scope.try_submit(|_| Ok::<_, ()>(()));
}
fn main() {}

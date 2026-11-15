// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// EXPECT: try_take_outcome
use qubit_operation::OperationObserver;
fn check(observer: OperationObserver) {
    let _ = observer.try_take_outcome();
}
fn main() {}

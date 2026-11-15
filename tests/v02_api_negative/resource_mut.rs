// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// EXPECT: DerefMut
use qubit_operation::ResourceRef;
fn check(mut resource: ResourceRef<std::sync::Mutex<usize>>) {
    *resource = std::sync::Mutex::new(3);
}

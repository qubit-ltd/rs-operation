// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use qubit_operation::ResourceRef;

fn mutate(resource: &mut ResourceRef<String>) {
    resource.push_str("forbidden");
}

fn main() {}

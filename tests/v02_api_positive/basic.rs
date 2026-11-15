// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use qubit_operation::ManagerOptions;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;

fn main() {
    let manager = OperationManager::new(ManagerOptions::default()).unwrap();
    let _operation = manager
        .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
        .unwrap();
}

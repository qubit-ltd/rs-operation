// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
// EXPECT: request_stop
use qubit_operation::OperationObserver;
use qubit_operation::StopReason;
fn check(observer: OperationObserver) {
    let _ = observer.request_stop(StopReason::User);
}

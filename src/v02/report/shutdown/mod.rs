// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

mod shutdown_mode;
pub use shutdown_mode::ShutdownMode;

mod shutdown_snapshot;
pub use shutdown_snapshot::ShutdownSnapshot;

mod shutdown_report;
pub use shutdown_report::ShutdownReport;

mod shutdown_wait;
pub use shutdown_wait::ShutdownWait;

// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Portable future driving, bounded blocking execution, and external driver leases.

#[path = "driver/external.rs"]
mod external;
#[path = "driver/internal/mod.rs"]
mod internal;
#[path = "driver/portable.rs"]
mod portable;
#[path = "driver/thread_executor.rs"]
mod thread_executor;

pub use external::DriverLease;
pub use external::DriverStatus;
pub use external::ExternalDriver;
pub use external::ExternalDriverError;
pub use external::ExternalDriverGuard;
pub use portable::PortableDriver;
pub use portable::PortableDriverConfig;
pub use portable::PortableDriverError;
pub use portable::TaskHandle;
pub use thread_executor::BlockingExecutor;
pub use thread_executor::ExecutorError;
pub use thread_executor::RejectedJob;
pub use thread_executor::ThreadExecutor;
pub use thread_executor::is_thread_executor_worker;

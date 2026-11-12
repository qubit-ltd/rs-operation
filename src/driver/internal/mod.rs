// =============================================================================
//    Copyright (c) 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

#[path = "portable_shared.rs"]
mod portable_shared;
#[path = "portable_state.rs"]
mod portable_state;
#[path = "portable_task.rs"]
mod portable_task;
// Waker implementation that schedules the corresponding portable task.
#[path = "portable_task_wake.rs"]
mod portable_task_wake;
#[path = "portable_worker.rs"]
mod portable_worker;
#[path = "thread_job.rs"]
mod thread_job;
#[path = "thread_shared.rs"]
mod thread_shared;
#[path = "thread_state.rs"]
mod thread_state;
#[path = "thread_worker.rs"]
mod thread_worker;

pub use portable_shared::PortableShared;
pub use portable_state::PortableState;
pub use portable_task::PortableTask;
pub use portable_task_wake::PortableTaskWake;
pub use portable_worker::close_portable;
pub use portable_worker::is_portable_worker_thread;
pub use portable_worker::portable_worker_loop;
pub use portable_worker::schedule_portable_task;
pub use thread_job::ThreadJob;
pub use thread_shared::ThreadShared;
pub use thread_state::ThreadState;
pub use thread_worker::close_thread_executor;
pub use thread_worker::current_thread_executor_id;
pub use thread_worker::next_executor_id;
pub use thread_worker::thread_worker_loop;

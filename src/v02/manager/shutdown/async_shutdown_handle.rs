// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::future::Future;
use std::task::Poll;

use super::ShutdownHandle;
use super::internal::ShutdownDeadline;
use super::internal::ShutdownTarget;
use crate::v02::protocol::Deadline;
use crate::v02::report::ObservationError;
use crate::v02::report::ShutdownReport;
use crate::v02::report::ShutdownSnapshot;
use crate::v02::report::ShutdownWait;

/// Async observer over the same manager shutdown state.
///
/// # Examples
///
/// ```
/// use qubit_operation::OperationManager;
/// use qubit_operation::{ManagerOptions, ShutdownMode};
///
/// let manager = OperationManager::new(ManagerOptions::default()).unwrap();
/// let shutdown = manager.request_shutdown(ShutdownMode::Drain);
/// let async_shutdown = shutdown.as_async();
/// assert_eq!(async_shutdown.snapshot().active_operations, 0);
/// ```
///
/// The async handle observes the same manager or group target as its sync
/// handle.
#[must_use = "retain this handle to observe async shutdown progress"]
#[derive(Clone)]
pub struct AsyncShutdownHandle {
    /// Synchronous observer holding the shared shutdown target.
    handle: ShutdownHandle,
}

impl AsyncShutdownHandle {
    /// Creates an async observer for an existing shutdown handle.
    ///
    /// # Parameters
    ///
    /// - `handle`: Synchronous observer whose target is shared.
    ///
    /// # Returns
    ///
    /// An async observer of the same shutdown target.
    #[inline]
    pub(super) fn from_handle(handle: ShutdownHandle) -> Self {
        Self { handle }
    }

    /// Returns a snapshot of the current shutdown progress.
    ///
    /// # Returns
    ///
    /// Counts of active and queued operations, cleanup, recovery, and service
    /// work.
    #[must_use = "inspect shutdown progress from this snapshot"]
    #[inline]
    pub fn snapshot(&self) -> ShutdownSnapshot {
        self.handle.snapshot()
    }

    /// Asynchronously waits until shutdown completes.
    ///
    /// Call this from outside an operation managed by the observed target.
    /// An operation cannot wait for its own completion; doing so returns
    /// [`ObservationError::WouldDeadlock`].
    ///
    /// # Returns
    ///
    /// A report containing the final shutdown snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ObservationError::WouldDeadlock`] when the current operation
    /// belongs to the target whose shutdown is being observed.
    #[must_use = "await and inspect the shutdown report"]
    pub async fn wait(&self) -> Result<ShutdownReport, ObservationError> {
        if self.handle.would_deadlock() {
            return Err(ObservationError::WouldDeadlock);
        }
        match &self.handle.target {
            ShutdownTarget::Manager(core) => core.shutdown_wait().await,
            ShutdownTarget::Group(_, group) => group.wait().await,
        }
        Ok(ShutdownReport {
            snapshot: self.snapshot(),
        })
    }

    /// Waits asynchronously until the target shuts down or `deadline` is
    /// reached.
    ///
    /// The deadline must use the same clock domain as the manager's clock.
    /// An expired deadline returns `Pending` immediately, including when the
    /// current operation belongs to the target.
    ///
    /// # Parameters
    ///
    /// - `deadline`: Absolute deadline for waiting on shutdown completion.
    ///
    /// # Returns
    ///
    /// `Completed` with a report if shutdown finishes, otherwise `Pending` with
    /// a snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`ObservationError::ClockUnavailable`] if no clock is
    /// configured, [`ObservationError::ClockDomainMismatch`] if the deadline
    /// uses another clock domain, or [`ObservationError::WouldDeadlock`] if
    /// waiting would depend on the current operation.
    pub async fn wait_until(&self, deadline: Deadline) -> Result<ShutdownWait, ObservationError> {
        let core = self.handle.manager();
        let clock = core.clock().ok_or(ObservationError::ClockUnavailable)?;
        if clock.now().domain() != deadline.time_point().domain() {
            return Err(ObservationError::ClockDomainMismatch);
        }
        if self.handle.is_empty() {
            if let ShutdownTarget::Manager(_) = &self.handle.target {
                core.finish_shutdown();
            }
            return Ok(ShutdownWait::Completed(ShutdownReport {
                snapshot: self.snapshot(),
            }));
        }
        if clock.now().ticks() >= deadline.time_point().ticks() {
            return Ok(ShutdownWait::Pending(self.snapshot()));
        }
        if self.handle.would_deadlock() {
            return Err(ObservationError::WouldDeadlock);
        }
        let mut completion = Box::pin(self.wait());
        let mut timer = Box::pin(ShutdownDeadline::new(clock, deadline));
        let completed = std::future::poll_fn(|cx| {
            match completion.as_mut().poll(cx) {
                Poll::Ready(Ok(_)) => return Poll::Ready(Ok(true)),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Ok(false));
            }
            Poll::Pending
        })
        .await;
        if completed? {
            Ok(ShutdownWait::Completed(ShutdownReport {
                snapshot: self.snapshot(),
            }))
        } else {
            Ok(ShutdownWait::Pending(self.snapshot()))
        }
    }

    /// Returns the synchronous observer for the same shutdown target.
    ///
    /// # Returns
    ///
    /// A synchronous handle sharing the shutdown target.
    #[must_use = "use the synchronous handle to observe shutdown progress"]
    #[inline]
    pub fn as_sync(&self) -> ShutdownHandle {
        self.handle.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use crate::v02::manager::core::OperationManager;
    use crate::v02::protocol::ClockDomainId;
    use crate::v02::protocol::ClockSource;
    use crate::v02::protocol::Deadline;
    use crate::v02::protocol::MonotonicTime;
    use crate::v02::protocol::TimerRegistration;
    use crate::v02::report::ManagerOptions;
    use crate::v02::report::ObservationError;
    use crate::v02::report::OperationOptions;
    use crate::v02::report::OperationOutcome;
    use crate::v02::report::ShutdownWait;

    struct FixedClock {
        now: Mutex<MonotonicTime>,
        advance_on_register: bool,
    }

    impl ClockSource for FixedClock {
        fn now(&self) -> MonotonicTime {
            *self.now.lock().unwrap()
        }

        fn register_deadline(&self, at: MonotonicTime, _wake: Waker) -> Box<dyn TimerRegistration> {
            if self.advance_on_register {
                *self.now.lock().unwrap() = at;
            }
            Box::new(NoopTimer)
        }
    }

    struct NoopTimer;

    impl TimerRegistration for NoopTimer {
        fn cancel(self: Box<Self>) {}
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = Box::pin(future);
        loop {
            let mut context = Context::from_waker(Waker::noop());
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn manager_with_clock(clock: Arc<dyn ClockSource>) -> OperationManager {
        OperationManager::new(ManagerOptions {
            clock: Some(clock),
            ..ManagerOptions::default()
        })
        .unwrap()
    }

    #[test]
    fn test_wait_until_checks_clock_and_completes_empty_shutdown() {
        let no_clock_manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let no_clock = no_clock_manager
            .request_shutdown(crate::v02::report::ShutdownMode::Drain)
            .as_async();
        assert!(matches!(
            block_on(no_clock.wait_until(Deadline::at(MonotonicTime::new(ClockDomainId::new_unique(), 2)))),
            Err(ObservationError::ClockUnavailable)
        ));

        let domain = ClockDomainId::new_unique();
        let clock = Arc::new(FixedClock {
            now: Mutex::new(MonotonicTime::new(domain, 1)),
            advance_on_register: false,
        });
        let shutdown = manager_with_clock(clock)
            .request_shutdown(crate::v02::report::ShutdownMode::Drain)
            .as_async();
        let mismatch = Deadline::at(MonotonicTime::new(ClockDomainId::new_unique(), 2));
        assert!(matches!(
            block_on(shutdown.wait_until(mismatch)),
            Err(ObservationError::ClockDomainMismatch)
        ));
        let completed = block_on(shutdown.wait_until(Deadline::at(MonotonicTime::new(domain, 1)))).unwrap();
        assert!(matches!(completed, ShutdownWait::Completed(_)));
        assert_eq!(shutdown.as_sync().snapshot().active_operations, 0);
    }

    #[test]
    fn test_wait_until_returns_pending_snapshot_when_timer_wins() {
        let domain = ClockDomainId::new_unique();
        let clock = Arc::new(FixedClock {
            now: Mutex::new(MonotonicTime::new(domain, 1)),
            advance_on_register: true,
        });
        let manager = manager_with_clock(clock);
        let (started_tx, started_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        let mut operation = manager
            .try_submit(OperationOptions::default(), move |_| {
                started_tx.send(()).unwrap();
                finish_rx.recv().unwrap();
                Ok::<_, ()>(())
            })
            .unwrap();
        started_rx.recv().unwrap();
        let shutdown = manager
            .request_shutdown(crate::v02::report::ShutdownMode::Drain)
            .as_async();
        let result = block_on(shutdown.wait_until(Deadline::at(MonotonicTime::new(domain, 5)))).unwrap();
        assert!(matches!(result, ShutdownWait::Pending(snapshot) if snapshot.active_operations == 1));
        finish_tx.send(()).unwrap();
        assert!(matches!(
            operation.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(())
        ));
        operation.wait_settlement().unwrap();
        assert!(matches!(
            shutdown.as_sync().wait_for(std::time::Duration::from_secs(2)).unwrap(),
            ShutdownWait::Completed(_)
        ));
    }

    #[test]
    fn test_wait_until_rejects_self_wait_but_expired_deadline_is_pending() {
        let domain = ClockDomainId::new_unique();
        let manager = manager_with_clock(Arc::new(FixedClock {
            now: Mutex::new(MonotonicTime::new(domain, 1)),
            advance_on_register: false,
        }));
        let inside = manager.as_async();
        let (result_tx, result_rx) = mpsc::channel();
        let mut operation = manager
            .as_async()
            .try_submit(OperationOptions::default(), move |_| async move {
                let shutdown = inside.request_shutdown(crate::v02::report::ShutdownMode::Drain);
                result_tx
                    .send(block_on(
                        shutdown.wait_until(Deadline::at(MonotonicTime::new(domain, 5))),
                    ))
                    .unwrap();
                Ok::<_, ()>(())
            })
            .unwrap()
            .into_sync();
        assert!(matches!(
            result_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),
            Err(ObservationError::WouldDeadlock)
        ));
        assert!(matches!(
            operation.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(())
        ));

        let manager = manager_with_clock(Arc::new(FixedClock {
            now: Mutex::new(MonotonicTime::new(domain, 5)),
            advance_on_register: false,
        }));
        let inside = manager.as_async();
        let (result_tx, result_rx) = mpsc::channel();
        let mut operation = manager
            .as_async()
            .try_submit(OperationOptions::default(), move |_| async move {
                let shutdown = inside.request_shutdown(crate::v02::report::ShutdownMode::Drain);
                result_tx
                    .send(block_on(
                        shutdown.wait_until(Deadline::at(MonotonicTime::new(domain, 5))),
                    ))
                    .unwrap();
                Ok::<_, ()>(())
            })
            .unwrap()
            .into_sync();
        assert!(matches!(
            result_rx.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),
            Ok(ShutdownWait::Pending(_))
        ));
        assert!(matches!(
            operation.wait_outcome().unwrap(),
            OperationOutcome::Succeeded(())
        ));
    }
}

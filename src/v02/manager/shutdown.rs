//! Explicit global manager shutdown and observation handles.

use crate::v02::manager::core::{ManagerCore, ManagerPhase};
use crate::v02::protocol::{ClockSource, Deadline, TimerRegistration};
use crate::v02::report::{
    ObservationError, ShutdownMode, ShutdownReport, ShutdownSnapshot, ShutdownWait,
};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// A durable observer for the manager-wide shutdown already in progress.
#[derive(Clone)]
pub struct ShutdownHandle {
    core: Arc<ManagerCore>,
}

impl ShutdownHandle {
    pub(crate) fn new(core: Arc<ManagerCore>) -> Self {
        Self { core }
    }

    pub fn mode(&self) -> ShutdownMode {
        match self.core.phase() {
            ManagerPhase::Cancelling | ManagerPhase::Closed => ShutdownMode::Cancel,
            ManagerPhase::Open | ManagerPhase::Draining => ShutdownMode::Drain,
        }
    }

    pub fn snapshot(&self) -> ShutdownSnapshot {
        let active = self.core.active_scope_count();
        ShutdownSnapshot {
            active_operations: active,
            queued_operations: 0,
            unconfirmed_users: 0,
            cleaning_resources: self.core.cleaning_resource_count(),
            recovery_groups: 0,
            service_resources: self.core.service_scope_count(),
        }
    }

    pub async fn wait(&self) -> ShutdownReport {
        self.core.shutdown_wait().await;
        ShutdownReport {
            snapshot: self.snapshot(),
        }
    }

    pub async fn wait_until(&self, deadline: Deadline) -> Result<ShutdownWait, ObservationError> {
        let clock = self
            .core
            .clock()
            .ok_or(ObservationError::ClockUnavailable)?;
        if clock.now().domain() != deadline.time_point().domain() {
            return Err(ObservationError::ClockDomainMismatch);
        }
        if self.core.is_shutdown_empty() {
            self.core.finish_shutdown();
            return Ok(ShutdownWait::Completed(ShutdownReport {
                snapshot: self.snapshot(),
            }));
        }
        let mut completion = Box::pin(self.core.shutdown_wait());
        let mut timer = Box::pin(ShutdownDeadline::new(clock, deadline));
        let completed = std::future::poll_fn(|cx| {
            if completion.as_mut().poll(cx).is_ready() {
                return Poll::Ready(true);
            }
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(false);
            }
            Poll::Pending
        })
        .await;
        if completed {
            Ok(ShutdownWait::Completed(ShutdownReport {
                snapshot: self.snapshot(),
            }))
        } else {
            Ok(ShutdownWait::Pending(self.snapshot()))
        }
    }

    pub fn blocking(&self) -> BlockingShutdown {
        BlockingShutdown {
            handle: self.clone(),
        }
    }
}

/// Blocking facade over the same manager shutdown state.
pub struct BlockingShutdown {
    handle: ShutdownHandle,
}

impl BlockingShutdown {
    pub fn wait_for(&self, timeout: Duration) -> ShutdownWait {
        if self.handle.core.shutdown_wait_for(timeout) {
            ShutdownWait::Completed(ShutdownReport {
                snapshot: self.handle.snapshot(),
            })
        } else {
            ShutdownWait::Pending(self.handle.snapshot())
        }
    }
}

struct ShutdownDeadline {
    clock: Arc<dyn ClockSource>,
    deadline: Deadline,
    registration: Option<Box<dyn TimerRegistration>>,
    waker: Option<Waker>,
}

impl ShutdownDeadline {
    fn new(clock: Arc<dyn ClockSource>, deadline: Deadline) -> Self {
        Self {
            clock,
            deadline,
            registration: None,
            waker: None,
        }
    }
}

impl Future for ShutdownDeadline {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let now = self.clock.now();
        if now.domain() != self.deadline.time_point().domain()
            || now.ticks() >= self.deadline.time_point().ticks()
        {
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            return Poll::Ready(());
        }
        if self.waker.as_ref().is_none_or(|w| !w.will_wake(cx.waker())) {
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            self.waker = Some(cx.waker().clone());
            self.registration = Some(
                self.clock
                    .register_deadline(self.deadline.time_point(), cx.waker().clone()),
            );
        }
        let now = self.clock.now();
        if now.domain() != self.deadline.time_point().domain()
            || now.ticks() >= self.deadline.time_point().ticks()
        {
            if let Some(registration) = self.registration.take() {
                registration.cancel();
            }
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

impl Drop for ShutdownDeadline {
    fn drop(&mut self) {
        if let Some(registration) = self.registration.take() {
            registration.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::v02::manager::core::OperationManager;
    use crate::v02::report::{
        AdmissionError, ManagerOptions, OperationOptions, ShutdownMode, ShutdownWait,
    };
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn shutdown_can_upgrade_not_reopen() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let drain = manager.request_shutdown(ShutdownMode::Drain);
        assert_eq!(drain.mode(), ShutdownMode::Drain);
        let cancel = manager.request_shutdown(ShutdownMode::Cancel);
        assert_eq!(cancel.mode(), ShutdownMode::Cancel);
        assert_eq!(manager.close_admission(), ());
        assert_eq!(
            manager.request_shutdown(ShutdownMode::Drain).mode(),
            ShutdownMode::Cancel
        );
        let rejected = manager.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
        assert!(matches!(
            rejected,
            Err(error) if error.reason() == AdmissionError::Closed
        ));
        assert!(matches!(
            cancel.blocking().wait_for(Duration::from_secs(2)),
            ShutdownWait::Completed(_)
        ));
    }

    #[test]
    fn bound_manager_shutdown_is_global() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let (bound_tx, bound_rx) = mpsc::channel();
        let mut operation = manager
            .try_submit(OperationOptions::default(), move |scope| {
                bound_tx.send(scope.manager()).unwrap();
                Ok::<_, ()>(())
            })
            .unwrap();
        let bound = bound_rx.recv().unwrap();
        operation.wait_outcome().unwrap();
        let shutdown = bound.request_shutdown(ShutdownMode::Drain);
        let rejected = manager.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()));
        assert!(matches!(
            rejected,
            Err(error) if error.reason() == AdmissionError::Closed
        ));
        assert!(matches!(
            shutdown.blocking().wait_for(Duration::from_secs(2)),
            ShutdownWait::Completed(_)
        ));
    }

    #[test]
    fn drain_rejects_root_allows_open_descendants() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let (bound_tx, bound_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        let mut parent = manager
            .try_submit(OperationOptions::default(), move |scope| {
                bound_tx.send(scope.manager()).unwrap();
                finish_rx.recv().unwrap();
                Ok::<_, ()>(())
            })
            .unwrap();
        let shutdown = manager.request_shutdown(ShutdownMode::Drain);
        assert!(
            matches!(manager.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(())),
            Err(error) if error.reason() == AdmissionError::Closed)
        );
        let bound = bound_rx.recv().unwrap();
        let mut child = bound
            .try_submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
            .unwrap();
        assert!(matches!(
            child.wait_outcome().unwrap(),
            crate::v02::report::OperationOutcome::Succeeded(())
        ));
        finish_tx.send(()).unwrap();
        assert!(matches!(
            parent.wait_outcome().unwrap(),
            crate::v02::report::OperationOutcome::Succeeded(())
        ));
        parent.wait_settlement().unwrap();
        assert!(matches!(
            shutdown.blocking().wait_for(Duration::from_secs(2)),
            ShutdownWait::Completed(_)
        ));
    }

    #[test]
    fn cancel_blocks_commit_and_propagates() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let (bound_tx, bound_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        let mut parent = manager
            .try_submit(OperationOptions::default(), move |scope| {
                bound_tx.send(scope.manager()).unwrap();
                finish_rx.recv().unwrap();
                Ok::<_, ()>(())
            })
            .unwrap();
        let bound = bound_rx.recv().unwrap();
        let shutdown = manager.request_shutdown(ShutdownMode::Cancel);
        assert!(
            matches!(bound.try_submit(OperationOptions::default(), |_| Ok::<_, ()>(())),
            Err(error) if error.reason() == AdmissionError::Stopped)
        );
        finish_tx.send(()).unwrap();
        assert!(matches!(
            parent.wait_outcome().unwrap(),
            crate::v02::report::OperationOutcome::Stopped(_)
        ));
        parent.wait_settlement().unwrap();
        assert!(matches!(
            shutdown.blocking().wait_for(Duration::from_secs(2)),
            ShutdownWait::Completed(_)
        ));
    }
}

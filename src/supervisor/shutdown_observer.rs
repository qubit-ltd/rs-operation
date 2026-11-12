// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Reusable observer for supervisor or service-group shutdown.

use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use crate::context::ContextError;
use crate::context::ContextOptions;
use crate::context::Deadline;
use crate::context::OperationContext;
use crate::protocol::ClockSource;

use super::OperationSupervisor;
use super::ShutdownMode;
use super::ShutdownObservation;
use super::ShutdownReport;
use super::internal::ShutdownSource;
use super::internal::service_group_lifecycle::{retry_closed_service_group_cleanups, start_service_resource_cleanup};
use super::internal::util::lock;
use super::shutdown_wait_registration::ShutdownWaitRegistration;

/// Reusable observer for a supervisor or service-group shutdown.
pub struct ShutdownObserver(pub(super) ShutdownSource);

impl Clone for ShutdownObserver {
    fn clone(&self) -> Self {
        Self(match &self.0 {
            ShutdownSource::Supervisor(supervisor) => ShutdownSource::Supervisor(supervisor.internal_handle()),
            ShutdownSource::Service(state, supervisor) => {
                ShutdownSource::Service(Arc::clone(state), supervisor.internal_handle())
            }
        })
    }
}

impl ShutdownObserver {
    /// Wait again for active work to settle, or return a fresh pending snapshot
    /// at deadline.
    pub async fn wait_async(
        &self,
        deadline: Deadline,
        clock: Arc<dyn ClockSource>,
    ) -> Result<ShutdownObservation, ContextError> {
        match &self.0 {
            ShutdownSource::Service(state, supervisor) => {
                start_service_resource_cleanup(Arc::clone(state), supervisor.internal_handle())
            }
            ShutdownSource::Supervisor(supervisor) => retry_closed_service_group_cleanups(supervisor),
        }
        let context = OperationContext::new(crate::context::ContextOptions {
            clock: Some(clock),
            ..Default::default()
        });
        let mut timer = context.signal_at(deadline)?;
        let mut registration = ShutdownWaitRegistration::new(self.0.clone());
        let report = core::future::poll_fn(|cx| {
            let report = self.snapshot();
            if report.is_complete() {
                return Poll::Ready(Ok(report));
            }
            if Pin::new(&mut timer).poll(cx).is_ready() {
                return Poll::Ready(Ok(report));
            }
            if let Err(error) = registration.register(cx.waker()) {
                return Poll::Ready(Err(error));
            }
            match &self.0 {
                ShutdownSource::Supervisor(supervisor) | ShutdownSource::Service(_, supervisor) => {
                    supervisor.register_recovery_wakers(cx.waker());
                }
            }
            if self.snapshot().is_complete() {
                return Poll::Ready(Ok(self.snapshot()));
            }
            Poll::Pending
        })
        .await?;
        registration.unregister();
        Ok(ShutdownObservation {
            report,
            observer: self.clone(),
        })
    }

    /// Wait synchronously for active work to settle or timeout.
    pub fn wait_blocking(&self, timeout: std::time::Duration) -> ShutdownObservation {
        match &self.0 {
            ShutdownSource::Service(state, supervisor) => {
                start_service_resource_cleanup(Arc::clone(state), supervisor.internal_handle())
            }
            ShutdownSource::Supervisor(supervisor) => retry_closed_service_group_cleanups(supervisor),
        }
        let report = match &self.0 {
            ShutdownSource::Supervisor(supervisor) => {
                let state = lock(&supervisor.inner.state);
                let (state, _) = supervisor
                    .inner
                    .changed
                    .wait_timeout_while(state, timeout, |s| {
                        s.active > 0
                            || !s.queued.is_empty()
                            || supervisor.service_totals().1 > 0
                            || supervisor.service_totals().0 > 0
                    })
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                ShutdownReport {
                    pending_requests: state.active + state.queued.len() + supervisor.service_totals().0,
                    pending_recovery: supervisor.pending_recovery(),
                    pending_resources: supervisor.service_totals().1,
                }
            }
            ShutdownSource::Service(state, supervisor) => {
                let (mutex, changed) = &**state;
                let guard = mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                let (guard, _) = changed
                    .wait_timeout_while(guard, timeout, |s| {
                        !s.requests.is_empty() || !s.resource_scopes.is_empty()
                    })
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                ShutdownReport {
                    pending_requests: guard.requests.len(),
                    pending_recovery: supervisor.pending_recovery(),
                    pending_resources: guard.resource_scopes.len(),
                }
            }
        };
        ShutdownObservation {
            report,
            observer: self.clone(),
        }
    }

    fn snapshot(&self) -> ShutdownReport {
        match &self.0 {
            ShutdownSource::Supervisor(supervisor) => {
                let state = lock(&supervisor.inner.state);
                ShutdownReport {
                    pending_requests: state.active + state.queued.len() + supervisor.service_totals().0,
                    pending_recovery: supervisor.pending_recovery(),
                    pending_resources: supervisor.service_totals().1,
                }
            }
            ShutdownSource::Service(state, supervisor) => {
                let guard = lock(&state.0);
                ShutdownReport {
                    pending_requests: guard.requests.len(),
                    pending_recovery: supervisor.pending_recovery(),
                    pending_resources: guard.resource_scopes.len(),
                }
            }
        }
    }
}


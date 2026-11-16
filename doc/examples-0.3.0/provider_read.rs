// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! A complete, controlled adapter lifecycle using a simulated provider.
//!
//! This example models a provider boundary; it does not read files or call a
//! real inference SDK. The application owns provider cancellation, input-use
//! completion, and its domain settlement report.

use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

use qubit_operation::ChildOutcomeKind;
use qubit_operation::ChildOutcomeSummary;
use qubit_operation::ChildRelease;
use qubit_operation::ChildSnapshot;
use qubit_operation::DiagnosticSummary;
use qubit_operation::EffectFact;
use qubit_operation::EffectRecord;
use qubit_operation::ExecutionFailure;
use qubit_operation::ExecutionPhase;
use qubit_operation::ExternalBindError;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationControl;
use qubit_operation::OperationId;
use qubit_operation::OperationManager;
use qubit_operation::OperationOptions;
use qubit_operation::OperationOutcome;
use qubit_operation::OperationScope;
use qubit_operation::PreparedExternalOperation;
use qubit_operation::ResourceRef;
use qubit_operation::SettlementReport;
use qubit_operation::SettlementStatus;
use qubit_operation::StopReason;
use qubit_operation::StopReport;

/// Request provider cancellation without claiming its input has been released.
pub fn request_provider_stop<T, E>(control: &impl OperationControl) {
    let _ = (std::marker::PhantomData::<(T, E)>, control);
    control.request_stop(StopReason::UserCancelled);
}

/// Classify an already-consumed typed result.
pub fn outcome_is_stopped<T, E>(outcome: &OperationOutcome<T, E>) -> bool {
    matches!(outcome, OperationOutcome::Stopped(_))
}

#[derive(Default)]
struct ProviderState {
    id: Option<OperationId>,
    active: bool,
    finished: bool,
    stop_requested: bool,
    outcome: Option<OperationOutcome<String, String>>,
    summary: Option<ChildOutcomeSummary>,
    input_released: bool,
    settlement: Option<Arc<SettlementReport>>,
    outcome_waker: Option<Waker>,
    summary_waker: Option<Waker>,
    release_waker: Option<Waker>,
    settlement_waker: Option<Waker>,
    input: Option<ResourceRef<SimulatedInput>>,
    scope: Option<OperationScope>,
}

/// Marker value representing input owned by the simulated provider.
struct SimulatedInput(Arc<AtomicUsize>);

impl Drop for SimulatedInput {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Observable result of the complete guide lifecycle demonstration.
pub struct ProviderLifecycleDemo {
    /// Manager report containing the provider's domain settlement.
    pub report: Arc<SettlementReport>,
    /// Number of input drops observed immediately before provider release.
    pub drops_before_release: usize,
    /// Number of input drops observed after manager settlement.
    pub drops_after_settlement: usize,
}

fn leaf_settlement(id: OperationId, description: &str) -> SettlementReport {
    SettlementReport {
        operation_id: id,
        status: SettlementStatus::Released,
        released_count: 0,
        transferred_count: 0,
        recovery_count: 0,
        children: Arc::from([]),
        children_total: 0,
        children_truncated: false,
        cleanup_failures: Arc::from([]),
        cleanup_failures_total: 0,
        cleanup_failures_truncated: false,
        resource_reports: Arc::from([]),
        resource_reports_total: 0,
        resource_reports_truncated: false,
        transfers: Arc::from([]),
        transfers_total: 0,
        transfers_truncated: false,
        recoveries: Arc::from([]),
        recoveries_total: 0,
        recoveries_truncated: false,
        effects: Arc::from([EffectRecord {
            operation_id: id,
            resource_id: None,
            fact: EffectFact::Known,
            description: DiagnosticSummary::new(description),
        }]),
        effects_total: 1,
        effect_overflow: 0,
        external_settlement: None,
    }
}

/// Application-owned prepared adapter shared with the manager driver.
pub struct SimulatedProvider {
    state: Mutex<ProviderState>,
    changed: Condvar,
    drops: Mutex<Option<Arc<AtomicUsize>>>,
}

impl Default for SimulatedProvider {
    fn default() -> Self {
        Self {
            state: Mutex::new(ProviderState::default()),
            changed: Condvar::new(),
            drops: Mutex::new(None),
        }
    }
}

impl SimulatedProvider {
    fn set_drop_counter(&self, drops: Arc<AtomicUsize>) {
        *self.drops.lock().expect("drop counter poisoned") = Some(drops);
    }

    fn publish_outcome(&self) {
        let (outcome_waker, summary_waker) = {
            let mut state = self.state.lock().expect("provider state poisoned");
            let id = state.id.expect("manager binds before activation");
            state.outcome = Some(OperationOutcome::Stopped(StopReport {
                reason: StopReason::UserCancelled,
                accepted_at: None,
            }));
            state.summary = Some(ChildOutcomeSummary {
                operation_id: id,
                kind: ChildOutcomeKind::Stopped,
                summary: DiagnosticSummary::new("simulated provider stopped; input still retained"),
            });
            (state.outcome_waker.take(), state.summary_waker.take())
        };
        self.changed.notify_all();
        if let Some(waker) = outcome_waker {
            waker.wake();
        }
        if let Some(waker) = summary_waker {
            waker.wake();
        }
    }

    fn finish_input_use(&self) {
        let (input, scope, release_waker) = {
            let mut state = self.state.lock().expect("provider state poisoned");
            state.input_released = true;
            state.active = false;
            state.finished = true;
            (state.input.take(), state.scope.take(), state.release_waker.take())
        };
        drop(input);
        drop(scope);
        self.changed.notify_all();
        if let Some(waker) = release_waker {
            waker.wake();
        }
    }

    fn publish_domain_settlement(&self) {
        let (waker, report) = {
            let mut state = self.state.lock().expect("provider state poisoned");
            let id = state.id.expect("manager binds before settlement");
            let report = Arc::new(SettlementReport {
                operation_id: id,
                status: SettlementStatus::Released,
                released_count: 0,
                transferred_count: 0,
                recovery_count: 0,
                children: Arc::from([]),
                children_total: 0,
                children_truncated: false,
                cleanup_failures: Arc::from([]),
                cleanup_failures_total: 0,
                cleanup_failures_truncated: false,
                resource_reports: Arc::from([]),
                resource_reports_total: 0,
                resource_reports_truncated: false,
                transfers: Arc::from([]),
                transfers_total: 0,
                transfers_truncated: false,
                recoveries: Arc::from([]),
                recoveries_total: 0,
                recoveries_truncated: false,
                effects: Arc::from([EffectRecord {
                    operation_id: id,
                    resource_id: None,
                    fact: EffectFact::Known,
                    description: DiagnosticSummary::new("provider request was cancelled"),
                }]),
                effects_total: 1,
                effect_overflow: 0,
                // This leaf application report must not wait on the manager's
                // final report, which itself incorporates this report.
                external_settlement: None,
            });
            state.settlement = Some(Arc::clone(&report));
            (state.settlement_waker.take(), report)
        };
        drop(report);
        self.changed.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn wait_for_activation(&self) -> ResourceRef<SimulatedInput> {
        let mut state = self.state.lock().expect("provider state poisoned");
        while !state.active {
            state = self.changed.wait(state).expect("provider state poisoned");
        }
        state.input.as_ref().expect("activation retains input").clone()
    }

    fn wait_for_outcome(&self) {
        let mut state = self.state.lock().expect("provider state poisoned");
        while state.outcome.is_none() {
            state = self.changed.wait(state).expect("provider state poisoned");
        }
    }

    /// Runs all provider phases and returns the manager's combined report and
    /// observable input drop counts.
    ///
    /// The manager driver runs independently while the application explicitly
    /// advances the simulated provider through stop, release, and settlement.
    fn run_lifecycle_demo() -> ProviderLifecycleDemo {
        let manager = OperationManager::new(ManagerOptions::default()).expect("manager creation");
        let drops = Arc::new(AtomicUsize::new(0));
        let provider = Arc::new(Self::default());
        provider.set_drop_counter(Arc::clone(&drops));
        let mut handle = manager
            .try_submit_external::<String, String, _>(OperationOptions::default(), Arc::clone(&provider))
            .expect("simulated provider admission");
        let input = provider.wait_for_activation();
        assert_eq!(
            handle.request_stop(StopReason::UserCancelled),
            qubit_operation::StopRequestStatus::Accepted,
            "the application initiates stop through its operation handle"
        );
        provider.publish_outcome();
        provider.wait_for_outcome();
        let _typed_outcome = handle.wait_outcome().expect("typed provider outcome");
        assert!(
            provider.state.lock().expect("provider state poisoned").input.is_some(),
            "publishing an outcome does not release the provider input"
        );
        assert!(
            matches!(
                handle
                    .wait_settlement_for(Duration::ZERO)
                    .expect("settlement observation"),
                qubit_operation::SettlementWait::Pending(_)
            ),
            "release and domain settlement are independent facts"
        );
        let drops_before_release = drops.load(Ordering::SeqCst);
        assert_eq!(drops_before_release, 0, "input must remain alive before release");
        drop(input);
        provider.finish_input_use();
        assert_eq!(
            provider.snapshot().execution,
            ExecutionPhase::Finished,
            "provider execution ends when it releases its input"
        );
        provider.publish_domain_settlement();
        let report = handle.wait_settlement().expect("combined settlement");
        let drops_after_settlement = drops.load(Ordering::SeqCst);
        assert_eq!(drops_after_settlement, 1, "input is dropped once after release");
        assert!(report.all_released());
        assert!(report.external_settlement.is_some(), "domain report is retained");
        drop(handle);
        let _shutdown_report = manager
            .request_shutdown(qubit_operation::ShutdownMode::Drain)
            .wait()
            .expect("manager shutdown");
        ProviderLifecycleDemo {
            report,
            drops_before_release,
            drops_after_settlement,
        }
    }
}

impl OperationControl for SimulatedProvider {
    fn request_stop(&self, _reason: StopReason) {
        let mut state = self.state.lock().expect("provider state poisoned");
        state.stop_requested = true;
        self.changed.notify_all();
    }

    fn snapshot(&self) -> ChildSnapshot {
        let state = self.state.lock().expect("provider state poisoned");
        ChildSnapshot {
            operation_id: state.id.expect("manager binds before polling"),
            execution: if state.finished {
                ExecutionPhase::Finished
            } else if state.active {
                ExecutionPhase::Running
            } else {
                ExecutionPhase::Queued
            },
            stop: state.stop_requested.then_some(StopReport {
                reason: StopReason::UserCancelled,
                accepted_at: None,
            }),
            outcome: state.summary.clone(),
            input_released: state.input_released,
            settlement: state.settlement.clone(),
        }
    }

    fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
        let mut state = self.state.lock().expect("provider state poisoned");
        if let Some(summary) = state.summary.clone() {
            Poll::Ready(summary)
        } else {
            let old_waker = state.summary_waker.replace(cx.waker().clone());
            self.changed.notify_all();
            drop(state);
            drop(old_waker);
            Poll::Pending
        }
    }

    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        let mut state = self.state.lock().expect("provider state poisoned");
        if state.input_released {
            Poll::Ready(ChildRelease {
                operation_id: state.id.expect("bound"),
                confirmed_at: None,
            })
        } else {
            let old_waker = state.release_waker.replace(cx.waker().clone());
            self.changed.notify_all();
            drop(state);
            drop(old_waker);
            Poll::Pending
        }
    }

    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>> {
        let mut state = self.state.lock().expect("provider state poisoned");
        if let Some(report) = state.settlement.clone() {
            Poll::Ready(report)
        } else {
            let old_waker = state.settlement_waker.replace(cx.waker().clone());
            self.changed.notify_all();
            drop(state);
            drop(old_waker);
            Poll::Pending
        }
    }
}

impl PreparedExternalOperation<String, String> for SimulatedProvider {
    fn bind(&self, id: OperationId) -> Result<(), ExternalBindError> {
        let mut state = self.state.lock().expect("provider state poisoned");
        if state.id.is_some() {
            return Err(ExternalBindError::AlreadyBound);
        }
        state.id = Some(id);
        Ok(())
    }

    fn activate(self: Arc<Self>, scope: OperationScope) -> Result<(), ExecutionFailure> {
        let id = self
            .state
            .lock()
            .expect("provider state poisoned")
            .id
            .expect("bound before activation");
        let drops = self
            .drops
            .lock()
            .expect("drop counter poisoned")
            .as_ref()
            .expect("demo configures counter before activation")
            .clone();
        let input = scope.manage(SimulatedInput(drops)).map_err(|_| ExecutionFailure {
            operation_id: id,
            summary: DiagnosticSummary::new("simulated input resource registration failed"),
            source: None,
        })?;
        let mut state = self.state.lock().expect("provider state poisoned");
        state.input = Some(input);
        state.scope = Some(scope);
        state.active = true;
        self.changed.notify_all();
        Ok(())
    }

    fn abandon(self: Arc<Self>) {
        // No resource is created before activation. Keep the bound identity and
        // publish a terminal pre-start disposition so any registered fact poll
        // can finish rather than waiting for a worker that was never started.
        let (outcome_waker, summary_waker, release_waker, settlement_waker) = {
            let mut state = self.state.lock().expect("provider state poisoned");
            let Some(id) = state.id else {
                return;
            };
            state.input_released = true;
            state.finished = true;
            state.outcome = Some(OperationOutcome::Stopped(StopReport {
                reason: StopReason::ManagerShutdown,
                accepted_at: None,
            }));
            state.summary = Some(ChildOutcomeSummary {
                operation_id: id,
                kind: ChildOutcomeKind::Stopped,
                summary: DiagnosticSummary::new("prepared provider was abandoned before activation"),
            });
            state.settlement = Some(Arc::new(leaf_settlement(id, "provider never started")));
            (
                state.outcome_waker.take(),
                state.summary_waker.take(),
                state.release_waker.take(),
                state.settlement_waker.take(),
            )
        };
        self.changed.notify_all();
        for waker in [outcome_waker, summary_waker, release_waker, settlement_waker]
            .into_iter()
            .flatten()
        {
            waker.wake();
        }
    }

    fn poll_take_outcome(&self, cx: &mut Context<'_>) -> Poll<OperationOutcome<String, String>> {
        let mut state = self.state.lock().expect("provider state poisoned");
        if let Some(outcome) = state.outcome.take() {
            Poll::Ready(outcome)
        } else {
            let old_waker = state.outcome_waker.replace(cx.waker().clone());
            self.changed.notify_all();
            drop(state);
            drop(old_waker);
            Poll::Pending
        }
    }
}

/// Runs the complete simulated provider lifecycle and manager shutdown.
pub fn demo_provider_lifecycle() -> ProviderLifecycleDemo {
    SimulatedProvider::run_lifecycle_demo()
}

/// Compatibility helper used by the short stop/settlement guide snippet.
pub fn request_worker_stop(control: &impl OperationControl) {
    control.request_stop(StopReason::UserCancelled);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::task::Context;
    use std::task::Poll;
    use std::task::Waker;

    use qubit_operation::ChildOutcomeKind;
    use qubit_operation::ChildOutcomeSummary;
    use qubit_operation::ChildRelease;
    use qubit_operation::ManagerOptions;
    use qubit_operation::OperationControl;
    use qubit_operation::OperationManager;
    use qubit_operation::OperationOptions;
    use qubit_operation::PreparedExternalOperation;

    use super::SimulatedProvider;
    use super::demo_provider_lifecycle;
    use super::outcome_is_stopped;
    use super::request_provider_stop;
    use super::request_worker_stop;

    #[test]
    fn test_demo_runs_complete_provider_lifecycle() {
        let demo = demo_provider_lifecycle();
        assert_eq!(demo.drops_before_release, 0);
        assert_eq!(demo.drops_after_settlement, 1);
        assert!(demo.report.all_released());
        let domain = demo
            .report
            .external_settlement
            .as_ref()
            .expect("provider domain settlement is retained");
        assert_eq!(domain.operation_id, demo.report.operation_id);
        assert!(domain.external_settlement.is_none());
    }

    #[test]
    fn test_abandon_publishes_unactivated_provider_facts() {
        let manager = OperationManager::new(ManagerOptions::default()).unwrap();
        let mut id_source = manager
            .submit(OperationOptions::default(), |_| Ok::<_, ()>(()))
            .unwrap();
        let id = id_source.id();
        let _outcome = id_source.wait_outcome().unwrap();
        id_source.wait_settlement().unwrap();

        let provider = Arc::new(SimulatedProvider::default());
        provider.bind(id).unwrap();
        request_provider_stop::<String, String>(provider.as_ref());
        request_worker_stop(provider.as_ref());
        PreparedExternalOperation::abandon(Arc::clone(&provider));
        let mut context = Context::from_waker(Waker::noop());

        let outcome = match provider.poll_take_outcome(&mut context) {
            Poll::Ready(outcome) => outcome,
            Poll::Pending => panic!("abandon publishes typed outcome without activation"),
        };
        assert!(outcome_is_stopped(&outcome));
        assert!(matches!(
            provider.poll_outcome(&mut context),
            Poll::Ready(ChildOutcomeSummary {
                operation_id,
                kind: ChildOutcomeKind::Stopped,
                ..
            }) if operation_id == id
        ));
        assert!(matches!(
            provider.poll_release(&mut context),
            Poll::Ready(ChildRelease { operation_id, .. }) if operation_id == id
        ));
        let settlement = match provider.poll_settlement(&mut context) {
            Poll::Ready(settlement) => settlement,
            Poll::Pending => panic!("abandon publishes settlement without activation"),
        };
        assert_eq!(settlement.operation_id, id);
        assert!(settlement.all_released());

        let _shutdown_report = manager
            .request_shutdown(qubit_operation::ShutdownMode::Drain)
            .wait()
            .unwrap();
    }
}

// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use super::TaskState;
use super::lock;
use super::poll_fact;
use super::wake_all;
use crate::v02::protocol::OperationControl;
use crate::v02::report::ChildOutcomeSummary;
use crate::v02::report::ChildRelease;
use crate::v02::report::ChildSnapshot;
use crate::v02::report::SettlementReport;
use crate::v02::report::StopReason;
use crate::v02::report::StopReport;

/// Cloneable observation port independent of the task's ownership.
///
/// # Type Parameters
///
/// * `T` is the input resource type observed by this control port.
pub(crate) struct ExternalTaskControl<T: Send + Sync + 'static> {
    /// Shared lifecycle facts retained with the task's input reference.
    pub(super) state: Arc<Mutex<TaskState<T>>>,
}

impl<T: Send + Sync + 'static> OperationControl for ExternalTaskControl<T> {
    /// Records the first stop request while the task is still running.
    ///
    /// # Parameters
    ///
    /// * `reason` identifies why the parent requests the task to stop.
    fn request_stop(&self, reason: StopReason) {
        let waiters = {
            let mut state = lock(&self.state);
            if state.stop.is_some() || state.execution == crate::v02::report::ExecutionPhase::Finished {
                return;
            }
            state.stop = Some(StopReport {
                reason,
                accepted_at: None,
            });
            Vec::new()
        };
        wake_all(waiters);
    }

    /// Returns a snapshot of the task's independently published facts.
    fn snapshot(&self) -> ChildSnapshot {
        let state = lock(&self.state);
        ChildSnapshot {
            operation_id: state.operation_id,
            execution: state.execution,
            stop: state.stop.clone(),
            outcome: state.outcome.clone(),
            input_released: state.release.is_some(),
            settlement: state.settlement.clone(),
        }
    }

    /// Polls the task outcome and registers an observer waker if pending.
    ///
    /// # Parameters
    ///
    /// * `cx` supplies the observer waker to register while the outcome is
    ///   pending.
    fn poll_outcome(&self, cx: &mut Context<'_>) -> Poll<ChildOutcomeSummary> {
        poll_fact(
            &self.state,
            cx,
            |state| &mut state.outcome_waiters,
            |state| state.outcome.clone(),
        )
    }

    /// Polls input-release evidence and registers an observer waker if pending.
    ///
    /// # Parameters
    ///
    /// * `cx` supplies the observer waker to register while release is pending.
    fn poll_release(&self, cx: &mut Context<'_>) -> Poll<ChildRelease> {
        poll_fact(
            &self.state,
            cx,
            |state| &mut state.release_waiters,
            |state| state.release.clone(),
        )
    }

    /// Polls settlement and registers an observer waker if pending.
    ///
    /// # Parameters
    ///
    /// * `cx` supplies the observer waker to register while settlement is
    ///   pending.
    fn poll_settlement(&self, cx: &mut Context<'_>) -> Poll<Arc<SettlementReport>> {
        poll_fact(
            &self.state,
            cx,
            |state| &mut state.settlement_waiters,
            |state| state.settlement.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::Context;
    use std::task::Poll;

    use super::ExternalTaskControl;
    use crate::v02::adapter::internal::TaskState;
    use crate::v02::protocol::OperationControl;
    use crate::v02::report::ChildOutcomeKind;
    use crate::v02::report::ChildOutcomeSummary;
    use crate::v02::report::DiagnosticSummary;
    use crate::v02::report::ExecutionPhase;
    use crate::v02::report::OperationId;

    #[test]
    fn test_external_task_control_polls_outcome_before_and_after_publication() {
        let operation_id = OperationId::new_unique();
        let state = Arc::new(Mutex::new(TaskState::<()> {
            operation_id,
            execution: ExecutionPhase::Running,
            stop: None,
            outcome: None,
            release: None,
            settlement: None,
            input: None,
            runtime_lost: false,
            outcome_waiters: Vec::new(),
            release_waiters: Vec::new(),
            settlement_waiters: Vec::new(),
        }));
        let control = ExternalTaskControl::<()> {
            state: Arc::clone(&state),
        };
        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert!(matches!(control.poll_outcome(&mut cx), Poll::Pending));
        assert_eq!(state.lock().unwrap().outcome_waiters.len(), 1);

        let outcome = ChildOutcomeSummary {
            operation_id,
            kind: ChildOutcomeKind::Succeeded,
            summary: DiagnosticSummary::new("completed"),
        };
        state.lock().unwrap().outcome = Some(outcome.clone());

        assert_eq!(control.poll_outcome(&mut cx), Poll::Ready(outcome));
    }
}

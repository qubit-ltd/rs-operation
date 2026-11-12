use core::future::Future;
use core::task::Context;
use core::task::Poll;

use qubit_operation::protocol::CleanupContext;
use qubit_operation::protocol::ClockSource;
use qubit_operation::protocol::ManagedOperationControl;
use qubit_operation::protocol::OwnedCleanup;
use qubit_operation::protocol::TimerRegistration;
use qubit_operation::report::AdmissionError;
use qubit_operation::report::ChildBusinessSummary;
use qubit_operation::report::ChildSettlement;
use qubit_operation::report::ChildSnapshot;
use qubit_operation::report::CleanupOutcome;
use qubit_operation::report::ClockDomainId;
use qubit_operation::report::DiagnosticSummary;
use qubit_operation::report::MonotonicTime;
use qubit_operation::report::OperationId;
use qubit_operation::report::OperationOutcome;
use qubit_operation::report::OperationPhase;
use qubit_operation::report::ResourcePhase;
use qubit_operation::report::ScopeLimits;
use qubit_operation::report::SettlementReport;
use qubit_operation::report::StartError;
use qubit_operation::report::StopReason;
use qubit_operation::report::SupervisorLimits;

struct Control;
impl ManagedOperationControl for Control {
    fn request_stop(&self, _: StopReason) {}
    fn snapshot(&self) -> ChildSnapshot {
        ChildSnapshot {
            operation_id: OperationId::new(1),
            phase: OperationPhase::Running,
            resource_phase: ResourcePhase::Active,
            stop_requested: false,
            released: false,
        }
    }
    fn poll_business(&self, _: &mut Context<'_>) -> Poll<ChildBusinessSummary> {
        Poll::Pending
    }
    fn poll_released(&self, _: &mut Context<'_>) -> Poll<ChildSettlement> {
        Poll::Pending
    }
}

struct Timer;
impl TimerRegistration for Timer {
    fn cancel(self: Box<Self>) {}
}
struct Clock;
impl ClockSource for Clock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::new(ClockDomainId(1), 0)
    }
    fn register_deadline(&self, _: MonotonicTime, _: std::task::Waker) -> Box<dyn TimerRegistration> {
        Box::new(Timer)
    }
}
struct Cleanup;
impl OwnedCleanup for Cleanup {
    fn begin(
        self: Box<Self>,
        _: CleanupContext,
    ) -> core::pin::Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
        Box::pin(async { CleanupOutcome::Cleaned })
    }
}

fn assert_control_object_safe(_: &dyn ManagedOperationControl) {}
fn assert_clock_object_safe(_: &dyn ClockSource) {}
fn assert_cleanup_object_safe(_: Box<dyn OwnedCleanup>) {}

#[test]
fn lifecycle_ports_are_object_safe_and_reports_do_not_require_clone_errors() {
    assert_control_object_safe(&Control);
    assert_clock_object_safe(&Clock);
    assert_cleanup_object_safe(Box::new(Cleanup));
    struct NotClone;
    let _: OperationOutcome<(), NotClone> = OperationOutcome::Failed(NotClone);
    let _ = core::mem::size_of::<AdmissionError>();
    let _ = core::mem::size_of::<SettlementReport>();
    let _ = OperationId::new(1);
    struct NonCloneBody;
    let rejected = StartError::new(AdmissionError::Busy, NonCloneBody);
    let _recovered_body: NonCloneBody = rejected.into_body();
    assert_eq!(ScopeLimits::default().child_operations, 64);
    assert_eq!(SupervisorLimits::default().unobserved_results, 64);
    assert!(DiagnosticSummary::new("诊断".repeat(200)).as_str().len() <= DiagnosticSummary::MAX_BYTES);
}

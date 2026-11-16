use qubit_operation::{OperationControl, OperationId, OperationScope, PreparedExternalOperation};
use std::sync::Arc;
use std::task::{Context, Poll};

struct Adapter;
impl OperationControl for Adapter {
    fn request_stop(&self, _: qubit_operation::StopReason) {}
    fn snapshot(&self) -> qubit_operation::ChildSnapshot {
        todo!()
    }
    fn poll_outcome(&self, _: &mut Context<'_>) -> Poll<qubit_operation::ChildOutcomeSummary> {
        Poll::Pending
    }
    fn poll_release(&self, _: &mut Context<'_>) -> Poll<qubit_operation::ChildRelease> {
        Poll::Pending
    }
    fn poll_settlement(&self, _: &mut Context<'_>) -> Poll<Arc<qubit_operation::SettlementReport>> {
        Poll::Pending
    }
}
impl PreparedExternalOperation<(), ()> for Adapter {
    fn bind(&self, _: OperationId) -> Result<(), qubit_operation::ExternalBindError> {
        todo!()
    }
    fn activate(
        self: Arc<Self>,
        _: OperationScope,
    ) -> Result<(), qubit_operation::ExecutionFailure> {
        todo!()
    }
    fn abandon(self: Arc<Self>) {
        todo!()
    }
    fn poll_take_outcome(
        &self,
        _: &mut Context<'_>,
    ) -> Poll<qubit_operation::OperationOutcome<(), ()>> {
        Poll::Pending
    }
}

#[test]
fn public_contract_symbols_are_importable_from_crate_root() {
    fn assert_sized<T: PreparedExternalOperation<(), ()>>() {}
    assert_sized::<Adapter>();
}

struct NoCloneNoDebug;
struct AdapterTyped;
impl OperationControl for AdapterTyped {
    fn request_stop(&self, _: qubit_operation::StopReason) {}
    fn snapshot(&self) -> qubit_operation::ChildSnapshot {
        todo!()
    }
    fn poll_outcome(&self, _: &mut Context<'_>) -> Poll<qubit_operation::ChildOutcomeSummary> {
        Poll::Pending
    }
    fn poll_release(&self, _: &mut Context<'_>) -> Poll<qubit_operation::ChildRelease> {
        Poll::Pending
    }
    fn poll_settlement(&self, _: &mut Context<'_>) -> Poll<Arc<qubit_operation::SettlementReport>> {
        Poll::Pending
    }
}
impl PreparedExternalOperation<NoCloneNoDebug, NoCloneNoDebug> for AdapterTyped {
    fn bind(&self, _: OperationId) -> Result<(), qubit_operation::ExternalBindError> {
        todo!()
    }
    fn activate(
        self: Arc<Self>,
        _: OperationScope,
    ) -> Result<(), qubit_operation::ExecutionFailure> {
        todo!()
    }
    fn abandon(self: Arc<Self>) {}
    fn poll_take_outcome(
        &self,
        _: &mut Context<'_>,
    ) -> Poll<qubit_operation::OperationOutcome<NoCloneNoDebug, NoCloneNoDebug>> {
        Poll::Pending
    }
}

#[test]
fn external_trait_is_object_safe_and_keeps_result_types_unconstrained() {
    fn takes_object(_: &dyn PreparedExternalOperation<NoCloneNoDebug, NoCloneNoDebug>) {}
    takes_object(&AdapterTyped);
    #[allow(dead_code)]
    fn invoke_object_callbacks(
        adapter: Arc<dyn PreparedExternalOperation<NoCloneNoDebug, NoCloneNoDebug>>,
        id: OperationId,
        scope: OperationScope,
        cx: &mut Context<'_>,
    ) {
        let _ = adapter.bind(id);
        let _ = adapter.clone().activate(scope);
        adapter.clone().abandon();
        let _ = adapter.poll_take_outcome(cx);
        adapter.request_stop(qubit_operation::StopReason::UserCancelled);
        let _ = adapter.snapshot();
        let _ = adapter.poll_outcome(cx);
        let _ = adapter.poll_release(cx);
        let _ = adapter.poll_settlement(cx);
    }
    let _ = invoke_object_callbacks;
    let adapter: Arc<dyn PreparedExternalOperation<NoCloneNoDebug, NoCloneNoDebug>> =
        Arc::new(AdapterTyped);
    assert_eq!(Arc::strong_count(&adapter), 1);
    #[allow(dead_code)]
    fn manager_entry_points_compile(
        sync: &qubit_operation::OperationManager,
        asynchronous: &qubit_operation::AsyncOperationManager,
        prepared: Arc<AdapterTyped>,
    ) {
        let _ = sync.try_submit_external::<NoCloneNoDebug, NoCloneNoDebug, _>(
            qubit_operation::OperationOptions::default(),
            Arc::clone(&prepared),
        );
        let _ = asynchronous.try_submit_external::<NoCloneNoDebug, NoCloneNoDebug, _>(
            qubit_operation::OperationOptions::default(),
            prepared,
        );
    }
}

#[test]
fn manager_group_limit_has_the_frozen_default() {
    assert_eq!(
        qubit_operation::ManagerOptions::default().max_groups_per_operation,
        16
    );
}

#[test]
fn adapter_error_and_already_bound_admission_are_public() {
    let _: Option<qubit_operation::ExternalBindError> = None;
    assert_eq!(
        qubit_operation::AdmissionError::AlreadyBound,
        qubit_operation::AdmissionError::AlreadyBound
    );
}

#[test]
fn scope_contract_signatures_are_publicly_frozen() {
    fn scope_methods(
        scope: &OperationScope,
        record: qubit_operation::EffectRecord,
    ) -> Result<(), qubit_operation::EffectRecordError> {
        let _: qubit_operation::StopRequestStatus =
            scope.request_stop(qubit_operation::StopReason::UserCancelled);
        scope.record_effect(record)
    }
    let _ = scope_methods;
    let _reserve: fn(
        &qubit_operation::ResourceScope,
    ) -> Result<
        qubit_operation::ResourceReservation,
        qubit_operation::ResourceCapacityError,
    > = qubit_operation::ResourceScope::reserve_resource;
    let _group: fn(
        &qubit_operation::OperationManager,
        &qubit_operation::OperationGroup,
    ) -> Result<
        qubit_operation::OperationManager,
        qubit_operation::GroupBindingError,
    > = qubit_operation::OperationManager::with_group;
}

#[test]
fn scope_and_resource_reference_restrictions_are_compile_time_contracts() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/ui/v03/*.rs");
}

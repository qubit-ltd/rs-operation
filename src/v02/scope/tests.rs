// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use std::sync::Arc;
use std::sync::Mutex;

use crate::v02::manager::core::ManagerCore;
use crate::v02::report::ManagerId;
use crate::v02::report::ResourceCapacityError;
use crate::v02::report::ResourceLookupError;
use crate::v02::report::ResourceManageErrorKind;
use crate::v02::report::ResourceScopeId;
use crate::v02::report::ResourceScopeOptions;
use crate::v02::report::StopReason;
use crate::v02::report::StopRequestStatus;
use crate::v02::resource::ResourceKey;

#[test]
fn test_scope_manager_is_owned_and_frame_bound() {
    let escaped = Arc::new(Mutex::new(None));
    let saved = Arc::clone(&escaped);
    let (_, manager) = ManagerCore::for_test();
    manager
        .try_submit_body(move |scope| {
            *saved.lock().unwrap() = Some(scope.manager());
        })
        .unwrap();
    let manager = escaped.lock().unwrap().take().unwrap();
    assert!(manager.try_submit_body(|_| {}).is_err());
}

#[test]
fn test_operation_scope_reports_resource_admission_failures_and_wrong_scope_lookup() {
    let (_, manager) = ManagerCore::for_test();
    manager
        .try_submit_body(|scope| {
            assert_eq!(scope.context().stop_reason(), None);

            let wrong_scope_key = ResourceKey::<String> {
                manager: ManagerId::new_unique(),
                scope: ResourceScopeId::new_unique(),
                slot: 0,
                generation: 0,
                type_id: std::any::TypeId::of::<String>(),
                marker: std::marker::PhantomData,
            };
            assert!(matches!(
                scope.get_resource(wrong_scope_key),
                Err(ResourceLookupError::WrongScope)
            ));
            assert!(matches!(
                scope.reserve_resource(),
                Err(ResourceCapacityError::CapacityExceeded)
            ));
            let error = match scope.manage(String::from("rejected")) {
                Ok(_) => panic!("test cleanup dispatcher must reject reservations"),
                Err(error) => error,
            };
            let (kind, value) = error.into_parts();
            assert_eq!(kind, ResourceManageErrorKind::CapacityExceeded);
            assert_eq!(value, "rejected");
            assert!(scope.resource_scope(ResourceScopeOptions { capacity: 0 }).is_err());
        })
        .unwrap();
}

#[test]
fn test_stopped_operation_scope_rejects_resource_admission_with_original_value() {
    let (_, manager) = ManagerCore::for_test();
    manager
        .try_submit_body(|scope| {
            assert_eq!(
                scope.request_stop(StopReason::UserCancelled),
                StopRequestStatus::Accepted
            );
            assert_eq!(
                scope.request_stop(StopReason::ParentStopped),
                StopRequestStatus::AlreadyRequested
            );
            assert_eq!(scope.context().stop_reason(), Some(StopReason::UserCancelled));

            assert!(matches!(scope.reserve_resource(), Err(ResourceCapacityError::Stopped)));
            let error = match scope.manage(String::from("rejected")) {
                Ok(_) => panic!("stopped scope must reject resource admission"),
                Err(error) => error,
            };
            let (kind, value) = error.into_parts();
            assert_eq!(kind, ResourceManageErrorKind::Stopped);
            assert_eq!(value, "rejected");
        })
        .unwrap();
}

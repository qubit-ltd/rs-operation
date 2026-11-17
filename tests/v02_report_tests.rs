// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

use qubit_operation::ClockDomainId;
use qubit_operation::DiagnosticSummary;
use qubit_operation::GroupBindingError;
use qubit_operation::ManagerLimits;
use qubit_operation::ManagerOptions;
use qubit_operation::OperationOptions;
use qubit_operation::RunError;
use qubit_operation::ScopeLimits;
use qubit_operation::SubmitError;

#[test]
fn test_display_identifies_each_group_binding_failure() {
    for (error, expected) in [
        (GroupBindingError::WrongManager, "WrongManager"),
        (GroupBindingError::Closed, "Closed"),
        (GroupBindingError::ScopeEnded, "ScopeEnded"),
        (GroupBindingError::CapacityExceeded, "CapacityExceeded"),
    ] {
        assert_eq!(error.to_string(), expected);
    }
}

#[test]
fn test_diagnostic_summary_truncates_only_at_utf8_boundaries() {
    let text = format!("{}é", "a".repeat(255));
    let summary = DiagnosticSummary::new(&text);

    assert_eq!(summary.as_str().len(), 255);
    assert!(summary.is_truncated());
    assert!(summary.as_str().is_char_boundary(summary.as_str().len()));
}

#[test]
fn test_clock_domain_ids_are_unique() {
    let first = ClockDomainId::new_unique();
    let second = ClockDomainId::new_unique();
    assert_ne!(first, second);
}

#[test]
fn test_default_limits_match_the_contract() {
    let manager = ManagerLimits::default();
    assert_eq!((manager.active_operations, manager.queued_operations), (4, 64));
    assert_eq!((manager.admission_waiters, manager.unconsumed_outcomes), (64, 64));
    assert_eq!((manager.cleanup_concurrency, manager.diagnostic_events), (4, 256));
    assert_eq!(
        ScopeLimits::default(),
        ScopeLimits {
            child_operations: 64,
            resources: 128,
            resource_scopes: 64,
            effect_records: 64
        }
    );
    assert_eq!(ManagerOptions::default().async_executor.task_capacity, 68);
    assert_eq!(OperationOptions::default().limits.resources, 128);
}

#[test]
fn test_submit_error_does_not_require_the_body_to_implement_debug() {
    struct NotDebug;
    fn assert_error<T: std::error::Error>() {}
    assert_error::<SubmitError<NotDebug>>();
    assert_error::<RunError<NotDebug>>();
}

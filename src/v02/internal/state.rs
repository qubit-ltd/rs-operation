// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================

//! Single-consumer result publication and stop arbitration.

mod internal;
mod result_future;
mod state_cell;

pub(in crate::v02::internal::state) use internal::ResultSlot;
pub(in crate::v02::internal::state) use internal::State;
pub(in crate::v02::internal::state) use internal::lock;
pub(crate) use result_future::ResultFuture;
pub(crate) use state_cell::StateCell;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Weak;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::task::Context;

    use super::StateCell;
    use crate::v02::report::OperationOutcome;
    use crate::v02::report::OutcomeError;

    struct LockCheckingDrop {
        cell: Weak<StateCell<LockCheckingDrop, ()>>,
        dropped_outside_lock: Arc<AtomicBool>,
    }

    impl Drop for LockCheckingDrop {
        fn drop(&mut self) {
            if let Some(cell) = self.cell.upgrade() {
                self.dropped_outside_lock
                    .store(cell.state.try_lock().is_ok(), Ordering::SeqCst);
            }
        }
    }

    #[test]
    fn test_discard_drops_ready_value_outside_state_lock() {
        let cell = Arc::new(StateCell::<LockCheckingDrop, ()>::new());
        let dropped_outside_lock = Arc::new(AtomicBool::new(false));
        let value = LockCheckingDrop {
            cell: Arc::downgrade(&cell),
            dropped_outside_lock: Arc::clone(&dropped_outside_lock),
        };
        assert!(cell.publish(OperationOutcome::Succeeded(value)).is_ok());
        assert!(cell.discard_result());
        assert!(dropped_outside_lock.load(Ordering::SeqCst));
    }

    #[test]
    fn test_cancelled_result_wait_keeps_delivery() {
        let cell = StateCell::<Box<u32>, ()>::new();
        let mut future = Box::pin(cell.result());
        let waker = std::task::Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(future.as_mut().poll(&mut context).is_pending());
        drop(future);

        assert!(cell.publish(OperationOutcome::Succeeded(Box::new(91))).is_ok());
        let outcome = cell
            .take_if_ready()
            .expect("cancelled observation must not take result")
            .expect("published result must remain available");
        assert!(matches!(outcome, OperationOutcome::Succeeded(value) if *value == 91));
    }

    #[test]
    fn test_discard_pending_prevents_later_delivery() {
        let cell = StateCell::<Box<u32>, ()>::new();
        assert!(cell.discard_result());
        let outcome = OperationOutcome::Succeeded(Box::new(17));
        let returned = cell
            .publish(outcome)
            .expect_err("discarded delivery must reject publication");
        assert!(matches!(returned, OperationOutcome::Succeeded(value) if *value == 17));
        assert_eq!(cell.take_if_ready().unwrap_err(), OutcomeError::Discarded);
    }

    #[test]
    fn test_take_once_preserves_owned_value() {
        let cell = StateCell::<Box<u32>, ()>::new();
        let owned = Box::new(73);
        assert!(cell.publish(OperationOutcome::Succeeded(owned)).is_ok());
        let received = cell
            .take_if_ready()
            .expect("result should be available")
            .expect("result should be ready");
        match received {
            OperationOutcome::Succeeded(value) => assert_eq!(*value, 73),
            _ => panic!("expected successful outcome"),
        }
        assert_eq!(cell.take_if_ready().unwrap_err(), OutcomeError::AlreadyTaken);
    }
}

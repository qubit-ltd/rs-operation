pub use crate::report::CleanupOutcome;
use core::{future::Future, pin::Pin};

/// Cleanup-only authority; its deadline does not inherit business cancellation.
#[derive(Clone, Copy, Debug)]
pub struct CleanupContext {
    /// Optional cleanup deadline in the configured clock domain.
    pub deadline: Option<crate::report::MonotonicTime>,
}

/// Owned cleanup action; the future cannot borrow the business stack.
///
/// Borrowed owners cannot cross the `'static` boundary:
/// ```compile_fail
/// use qubit_operation::protocol::{CleanupContext, CleanupOutcome, OwnedCleanup};
/// use std::{future::Future, pin::Pin};
/// struct Borrowed<'a>(&'a str);
/// impl<'a> OwnedCleanup for Borrowed<'a> {
///     fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
///         Box::pin(async move { let _: &'static str = self.0; CleanupOutcome::Cleaned })
///     }
/// }
/// ```
///
/// Non-Send owners cannot cross the managed cleanup boundary:
/// ```compile_fail
/// use qubit_operation::protocol::{CleanupContext, CleanupOutcome, OwnedCleanup};
/// use std::{future::Future, pin::Pin, rc::Rc};
/// struct Local(Rc<()>);
/// impl OwnedCleanup for Local {
///     fn begin(self: Box<Self>, _: CleanupContext) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>> {
///         Box::pin(async move { let _ = self.0; CleanupOutcome::Cleaned })
///     }
/// }
/// ```
pub trait OwnedCleanup: Send + 'static {
    /// Consume the cleanup action and produce an owned, independently driven future.
    /// The future must report only confirmed cleanup, accepted transfer, or a
    /// failure that retains recovery ownership.
    fn begin(
        self: Box<Self>,
        context: CleanupContext,
    ) -> Pin<Box<dyn Future<Output = CleanupOutcome> + Send + 'static>>;
}

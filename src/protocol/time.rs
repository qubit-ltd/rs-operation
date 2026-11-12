use crate::report::MonotonicTime;
use std::task::Waker;

/// Cancellation handle for one deadline registration.
pub trait TimerRegistration: Send + 'static {
    /// Cancel this registration only; it must not affect a later registration.
    fn cancel(self: Box<Self>);
}

/// Clock domain and timer source shared by deadline users.
pub trait ClockSource: Send + Sync {
    /// Read the current time in this source's stable clock domain.
    fn now(&self) -> MonotonicTime;
    /// Register a wakeup; the registration is cancelled by consuming its handle.
    fn register_deadline(&self, at: MonotonicTime, wake: Waker) -> Box<dyn TimerRegistration>;
}

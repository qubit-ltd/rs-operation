use core::fmt;

/// Failure to accept work before its body has been started.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Failure to accept work before its body has been started.
pub enum AdmissionError {
    /// All root, queue, or waiter capacity is reserved.
    Busy,
    /// New work is no longer accepted.
    Closed,
    /// The operation deadline elapsed before admission.
    DeadlineExpired,
    /// A finite child/resource registration budget is exhausted.
    ReservationLimit,
    /// No driver can accept the work.
    DriverUnavailable,
}

impl fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Busy => "operation capacity is busy",
            Self::Closed => "operation supervisor is closed",
            Self::DeadlineExpired => "operation deadline has expired",
            Self::ReservationLimit => "operation reservation limit reached",
            Self::DriverUnavailable => "operation driver is unavailable",
        })
    }
}

impl std::error::Error for AdmissionError {}

/// Rejected start request retaining the body that was never started.
/// The original body is recoverable because it has not been invoked.
pub struct StartError<F> {
    reason: AdmissionError,
    body: F,
}

impl<F> StartError<F> {
    /// Create a rejection while retaining the unstarted body.
    pub const fn new(reason: AdmissionError, body: F) -> Self {
        Self { reason, body }
    }
    /// Return the admission cause without consuming the retained body.
    pub const fn reason(&self) -> AdmissionError {
        self.reason
    }
    /// Recover the exact body supplied to the rejected start request.
    pub fn into_body(self) -> F {
        self.body
    }
    /// Recover both the admission cause and the exact unstarted body.
    pub fn into_parts(self) -> (AdmissionError, F) {
        (self.reason, self.body)
    }
}

impl<F> fmt::Debug for StartError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StartError")
            .field("reason", &self.reason)
            .finish_non_exhaustive()
    }
}
impl<F> fmt::Display for StartError<F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.reason.fmt(f)
    }
}
impl<F> std::error::Error for StartError<F> {}

/// Failure while observing a result or settlement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Failure while observing a result or settlement.
pub enum ObservationError {
    /// The one-shot typed result was already consumed.
    AlreadyTaken,
    /// The service discarded the typed result while retaining settlement responsibility.
    ResultDiscarded,
    /// A blocking observation from a framework-managed worker would deadlock.
    WouldDeadlock,
    /// The observer does not refer to a registered operation.
    InvalidObserver,
}

impl fmt::Display for ObservationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AlreadyTaken => "operation result was already taken",
            Self::ResultDiscarded => "operation result was discarded",
            Self::WouldDeadlock => "blocking observation would deadlock",
            Self::InvalidObserver => "operation observer is invalid",
        })
    }
}

impl std::error::Error for ObservationError {}

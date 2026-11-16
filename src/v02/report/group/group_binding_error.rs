use std::error::Error;
use std::fmt;

/// Error returned when binding a manager view to a service group is invalid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupBindingError {
    /// The group belongs to another manager.
    WrongManager,
    /// The group has stopped accepting operations.
    Closed,
    /// The parent operation scope has ended.
    ScopeEnded,
    /// The configured distinct-group limit would be exceeded.
    CapacityExceeded,
}

impl fmt::Display for GroupBindingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for GroupBindingError {}

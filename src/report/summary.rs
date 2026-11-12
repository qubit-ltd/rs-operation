use std::fmt;
use std::sync::Arc;

/// UTF-8 diagnostic text truncated to a fixed byte limit.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DiagnosticSummary(Arc<str>);

impl DiagnosticSummary {
    /// Maximum retained UTF-8 bytes per summary.
    pub const MAX_BYTES: usize = 256;

    /// Retain at most `MAX_BYTES`, truncating only at a UTF-8 boundary.
    pub fn new(value: impl AsRef<str>) -> Self {
        let value = value.as_ref();
        if value.len() <= Self::MAX_BYTES {
            return Self(Arc::from(value));
        }
        let mut end = Self::MAX_BYTES;
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        Self(Arc::from(&value[..end]))
    }

    /// Borrow the retained diagnostic text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for DiagnosticSummary {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for DiagnosticSummary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

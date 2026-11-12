/// Finite capacity reserved for one root operation and its descendants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Per-root finite registration capacity. Exhaustion rejects registration before work starts.
pub struct ScopeLimits {
    /// Maximum child-operation registrations, including attached but unreleased children.
    pub child_operations: usize,
    /// Maximum resource registrations, including resources retained for recovery.
    pub resources: usize,
    /// Maximum caller-declared side-effect records retained in the root settlement.
    pub effect_records: usize,
}

impl Default for ScopeLimits {
    fn default() -> Self {
        Self {
            child_operations: 64,
            resources: 128,
            effect_records: 64,
        }
    }
}

/// Finite supervisor-wide capacities from the 0.1 design baseline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Supervisor-wide finite queue, worker, and report capacities.
pub struct SupervisorLimits {
    /// Maximum concurrently active root scopes.
    pub active_roots: usize,
    /// Maximum root bodies accepted but waiting to run.
    pub queued_roots: usize,
    /// Maximum callers waiting for admission; excess callers receive `Busy`.
    pub admission_waiters: usize,
    /// Maximum concurrent cleanup tasks, separate from business admission.
    pub cleanup_concurrency: usize,
    /// Maximum retained diagnostic events; overflow must remain observable.
    pub unobserved_events: usize,
    /// Maximum reserved but unconsumed typed results.
    pub unobserved_results: usize,
    /// Default per-root child and resource capacities.
    pub scope: ScopeLimits,
}

impl Default for SupervisorLimits {
    fn default() -> Self {
        Self {
            active_roots: 4,
            queued_roots: 64,
            admission_waiters: 64,
            cleanup_concurrency: 4,
            unobserved_events: 256,
            unobserved_results: 64,
            scope: ScopeLimits::default(),
        }
    }
}

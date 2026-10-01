use std::sync::atomic::{AtomicBool, Ordering};

/// Lets one run of a periodic background task be in flight at a time. A run that waits on
/// the GIL can outlast its interval while the main thread is busy, and the cron would
/// otherwise start another one every tick.
pub struct SingleFlight(AtomicBool);

impl Default for SingleFlight {
    fn default() -> Self {
        Self::new()
    }
}

impl SingleFlight {
    pub const fn new() -> Self {
        Self(AtomicBool::new(false))
    }

    /// Starts a run, or `None` while another one holds the flight. The run ends when the
    /// returned [`Flight`] drops.
    pub fn try_start(&'static self) -> Option<Flight> {
        self.0
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
            // Lazily: an eagerly built `Flight` would drop on the losing path and end the
            // winner's run.
            .then(|| Flight(&self.0))
    }

    /// Ends a run whose [`Flight`] a test leaked.
    #[cfg(test)]
    pub fn reset(&self) {
        self.0.store(false, Ordering::Release);
    }
}

/// A run of a [`SingleFlight`] task; the next one may start once this drops.
pub struct Flight(&'static AtomicBool);

impl Drop for Flight {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::SingleFlight;

    #[test]
    fn one_run_at_a_time() {
        static TASK: SingleFlight = SingleFlight::new();
        let first = TASK.try_start().expect("idle task starts");
        assert!(TASK.try_start().is_none());
        drop(first);
        assert!(TASK.try_start().is_some());
    }

    #[test]
    fn rejected_starts_leave_the_run_in_flight() {
        static TASK: SingleFlight = SingleFlight::new();
        let first = TASK.try_start().expect("idle task starts");
        for _ in 0..3 {
            assert!(TASK.try_start().is_none());
        }
        drop(first);
        assert!(TASK.try_start().is_some());
    }
}

//! Lock-free timing of the time spent inside geyser plugin callbacks.
//!
//! Plugins run synchronously on validator threads (execution, replay,
//! retransmit, ...), so a slow plugin directly delays the pipeline. Each
//! [`NotifyTimings`] accumulates call count, total and maximum time and
//! reports them as a datapoint at most once per [`REPORT_INTERVAL_MS`].

use {
    solana_metrics::datapoint_info,
    std::{
        sync::{
            LazyLock,
            atomic::{AtomicU64, Ordering},
        },
        time::Instant,
    },
};

const REPORT_INTERVAL_MS: u64 = 2_000;

static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

pub(crate) struct NotifyTimings {
    datapoint_name: &'static str,
    count: AtomicU64,
    total_us: AtomicU64,
    max_us: AtomicU64,
    last_report_ms: AtomicU64,
}

impl NotifyTimings {
    pub(crate) const fn new(datapoint_name: &'static str) -> Self {
        Self {
            datapoint_name,
            count: AtomicU64::new(0),
            total_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            last_report_ms: AtomicU64::new(0),
        }
    }

    /// Records one plugin notification that started at `start` and ends now.
    pub(crate) fn record(&self, start: Instant) {
        let elapsed_us = start.elapsed().as_micros() as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_us.fetch_add(elapsed_us, Ordering::Relaxed);
        self.max_us.fetch_max(elapsed_us, Ordering::Relaxed);
        self.maybe_report(EPOCH.elapsed().as_millis() as u64);
    }

    fn maybe_report(&self, now_ms: u64) {
        let last = self.last_report_ms.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last) < REPORT_INTERVAL_MS
            || self
                .last_report_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
        {
            return;
        }
        let count = self.count.swap(0, Ordering::Relaxed);
        let total_us = self.total_us.swap(0, Ordering::Relaxed);
        let max_us = self.max_us.swap(0, Ordering::Relaxed);
        datapoint_info!(
            self.datapoint_name,
            ("count", count as i64, i64),
            ("total_us", total_us as i64, i64),
            ("max_us", max_us as i64, i64),
            (
                "avg_us",
                total_us.checked_div(count).unwrap_or(0) as i64,
                i64
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_accumulates() {
        let timings = NotifyTimings::new("test-notify-timings");
        timings.record(Instant::now());
        timings.record(Instant::now());
        // Nothing is reported before the interval has elapsed.
        assert_eq!(timings.count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_report_resets_counters_once_per_interval() {
        let timings = NotifyTimings::new("test-notify-timings");
        timings.count.store(5, Ordering::Relaxed);
        timings.total_us.store(50, Ordering::Relaxed);
        timings.max_us.store(20, Ordering::Relaxed);

        timings.maybe_report(REPORT_INTERVAL_MS - 1);
        assert_eq!(timings.count.load(Ordering::Relaxed), 5);

        timings.maybe_report(REPORT_INTERVAL_MS);
        assert_eq!(timings.count.load(Ordering::Relaxed), 0);
        assert_eq!(timings.total_us.load(Ordering::Relaxed), 0);
        assert_eq!(timings.max_us.load(Ordering::Relaxed), 0);

        // The next report is a full interval later.
        timings.count.store(1, Ordering::Relaxed);
        timings.maybe_report(REPORT_INTERVAL_MS + 1);
        assert_eq!(timings.count.load(Ordering::Relaxed), 1);
    }
}

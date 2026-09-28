//! Sampled end-to-end latency tracking for the shred -> deshred geyser path.
//!
//! Every stage metric elsewhere in the pipeline is a summed duration, which
//! cannot tell how long a shred takes to get from the fetch stage to a geyser
//! plugin. This module follows the data shreds of a sampled subset of slots
//! through the stages and reports, per completed data set, how long the
//! *last arriving* shred of the set took to reach the deshred notifier:
//!
//! ```text
//! fetch (modifier) -> sigverify out -> blockstore insert done
//!    -> completed-data-sets service dequeue -> entries loaded -> notified
//! ```
//!
//! Timestamps are taken in the following places:
//! - `mark_fetched`: `ShredFetchStage` packet modifier (after the batch has
//!   been dequeued, so socket batching time is not included; see the
//!   `fetch_max_batch_us` streamer metric for that).
//! - `mark_sigverified`: `turbine::sigverify_shreds`, right before handing the
//!   shreds to the window service.
//! - `mark_inserted`: `WindowService::run_insert`, after the blockstore write.
//! - `finish`: `CompletedDataSetsService`, after deshred notification.
//!
//! Only turbine data shreds are tracked. Data sets whose last shred was
//! recovered by erasure coding or repaired have no fetch timestamp for the
//! recovered shred and are measured from the last shred that did arrive.
//!
//! Configuration (environment variables, read once at first use):
//! - `AGAVE_PIPELINE_LATENCY_SLOT_SAMPLE`: track every N-th slot. `0`
//!   disables tracking, default is [`DEFAULT_SLOT_SAMPLE`].

use {
    crate::shred::{ShredType, layout},
    dashmap::DashMap,
    solana_clock::Slot,
    std::{
        sync::{
            LazyLock, Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    },
};

/// Track one out of this many slots unless overridden by the environment.
const DEFAULT_SLOT_SAMPLE: u64 = 2;
const SLOT_SAMPLE_ENV: &str = "AGAVE_PIPELINE_LATENCY_SLOT_SAMPLE";
const REPORT_INTERVAL: Duration = Duration::from_secs(10);
/// Untracked shreds/data sets older than this many slots behind the newest
/// fetched slot are dropped so the maps cannot grow without bound.
const MAX_SLOT_AGE: u64 = 64;

pub static PIPELINE_LATENCY: LazyLock<PipelineLatencyTracker> =
    LazyLock::new(PipelineLatencyTracker::from_env);

#[derive(Clone, Copy)]
struct ShredTimes {
    fetched: Instant,
    sigverified: Option<Instant>,
}

/// Timestamps taken by the completed data sets service.
#[derive(Clone, Copy)]
pub struct DeshredStageTimes {
    /// When the batch containing this data set was received off the channel.
    pub dequeued: Instant,
    /// When the entries of the data set were loaded and deserialized.
    pub entries_loaded: Instant,
}

#[derive(Default)]
struct Samples {
    fetch_to_sigverify_us: Vec<u64>,
    sigverify_to_insert_us: Vec<u64>,
    insert_to_dequeue_us: Vec<u64>,
    dequeue_to_loaded_us: Vec<u64>,
    loaded_to_notified_us: Vec<u64>,
    total_us: Vec<u64>,
    arrival_spread_us: Vec<u64>,
    untracked_data_sets: u64,
}

pub struct PipelineLatencyTracker {
    /// 0 means disabled.
    slot_sample: u64,
    max_fetched_slot: AtomicU64,
    shreds: DashMap<(Slot, u32), ShredTimes>,
    /// Keyed by (slot, first shred index of the data set).
    inserted: DashMap<(Slot, u32), Instant>,
    samples: Mutex<Samples>,
    last_report: Mutex<Instant>,
}

impl PipelineLatencyTracker {
    fn from_env() -> Self {
        let slot_sample = std::env::var(SLOT_SAMPLE_ENV)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_SLOT_SAMPLE);
        Self::new(slot_sample)
    }

    fn new(slot_sample: u64) -> Self {
        Self {
            slot_sample,
            max_fetched_slot: AtomicU64::default(),
            shreds: DashMap::default(),
            inserted: DashMap::default(),
            samples: Mutex::default(),
            last_report: Mutex::new(Instant::now()),
        }
    }

    #[inline]
    fn is_sampled(&self, slot: Slot) -> bool {
        self.slot_sample != 0 && slot % self.slot_sample == 0
    }

    /// Returns (slot, index) if `shred` is a data shred of a sampled slot.
    #[inline]
    fn sampled_data_shred_key(&self, shred: &[u8]) -> Option<(Slot, u32)> {
        if self.slot_sample == 0 {
            return None;
        }
        let slot = layout::get_slot(shred)?;
        if !self.is_sampled(slot) || layout::get_shred_type(shred).ok()? != ShredType::Data {
            return None;
        }
        Some((slot, layout::get_index(shred)?))
    }

    /// Records the time a turbine data shred passed the fetch stage.
    /// Keeps the first timestamp if the shred is seen more than once.
    pub fn mark_fetched(&self, now: Instant, shred: &[u8]) {
        let Some(key) = self.sampled_data_shred_key(shred) else {
            return;
        };
        self.max_fetched_slot.fetch_max(key.0, Ordering::Relaxed);
        self.shreds.entry(key).or_insert(ShredTimes {
            fetched: now,
            sigverified: None,
        });
    }

    /// Records the time turbine data shreds left sigverify.
    pub fn mark_sigverified<'a>(&self, shreds: impl Iterator<Item = &'a [u8]>) {
        if self.slot_sample == 0 {
            return;
        }
        let now = Instant::now();
        for shred in shreds {
            let Some(key) = self.sampled_data_shred_key(shred) else {
                continue;
            };
            if let Some(mut times) = self.shreds.get_mut(&key) {
                times.sigverified.get_or_insert(now);
            }
        }
    }

    /// Records the time the blockstore finished inserting the given data sets.
    pub fn mark_inserted<'a>(
        &self,
        completed_data_sets: impl Iterator<Item = (Slot, &'a std::ops::Range<u32>)>,
    ) {
        if self.slot_sample == 0 {
            return;
        }
        let now = Instant::now();
        for (slot, indices) in completed_data_sets {
            if self.is_sampled(slot) {
                self.inserted.entry((slot, indices.start)).or_insert(now);
            }
        }
    }

    /// Called once the deshred notifications of a data set have been sent.
    /// Computes the stage latencies of the last arriving shred of the set and
    /// periodically reports percentiles.
    pub fn finish(&self, slot: Slot, indices: &std::ops::Range<u32>, stages: DeshredStageTimes) {
        if !self.is_sampled(slot) {
            return;
        }
        let notified = Instant::now();
        let inserted = self.inserted.remove(&(slot, indices.start)).map(|(_, v)| v);
        let mut first_fetch: Option<Instant> = None;
        let mut last: Option<ShredTimes> = None;
        for index in indices.clone() {
            if let Some((_, times)) = self.shreds.remove(&(slot, index)) {
                first_fetch = Some(first_fetch.map_or(times.fetched, |f| f.min(times.fetched)));
                if last.is_none_or(|l| times.fetched > l.fetched) {
                    last = Some(times);
                }
            }
        }
        {
            let mut samples = self.samples.lock().unwrap();
            match (last, first_fetch, inserted) {
                (
                    Some(ShredTimes {
                        fetched,
                        sigverified: Some(sigverified),
                    }),
                    Some(first_fetch),
                    Some(inserted),
                ) => {
                    let us = |later: Instant, earlier: Instant| {
                        later.saturating_duration_since(earlier).as_micros() as u64
                    };
                    samples.fetch_to_sigverify_us.push(us(sigverified, fetched));
                    samples
                        .sigverify_to_insert_us
                        .push(us(inserted, sigverified));
                    samples
                        .insert_to_dequeue_us
                        .push(us(stages.dequeued, inserted));
                    samples
                        .dequeue_to_loaded_us
                        .push(us(stages.entries_loaded, stages.dequeued));
                    samples
                        .loaded_to_notified_us
                        .push(us(notified, stages.entries_loaded));
                    samples.total_us.push(us(notified, fetched));
                    samples.arrival_spread_us.push(us(fetched, first_fetch));
                }
                _ => samples.untracked_data_sets += 1,
            }
        }
        self.maybe_report(notified);
    }

    fn maybe_report(&self, now: Instant) {
        {
            let mut last_report = self.last_report.lock().unwrap();
            if now.saturating_duration_since(*last_report) < REPORT_INTERVAL {
                return;
            }
            *last_report = now;
        }
        let samples = std::mem::take(&mut *self.samples.lock().unwrap());
        self.prune();
        report(samples);
    }

    fn prune(&self) {
        let min_slot = self
            .max_fetched_slot
            .load(Ordering::Relaxed)
            .saturating_sub(MAX_SLOT_AGE);
        self.shreds.retain(|(slot, _), _| *slot >= min_slot);
        self.inserted.retain(|(slot, _), _| *slot >= min_slot);
    }
}

fn percentile(sorted: &[u64], pct: usize) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (sorted.len() * pct / 100).min(sorted.len() - 1);
    sorted[idx] as i64
}

fn report(mut samples: Samples) {
    let count = samples.total_us.len();
    let untracked = samples.untracked_data_sets;
    if count == 0 && untracked == 0 {
        return;
    }
    let stat = |v: &mut Vec<u64>| {
        v.sort_unstable();
        [
            percentile(v, 50),
            percentile(v, 90),
            percentile(v, 99),
            v.last().copied().unwrap_or(0) as i64,
        ]
    };
    let fetch_to_sigverify = stat(&mut samples.fetch_to_sigverify_us);
    let sigverify_to_insert = stat(&mut samples.sigverify_to_insert_us);
    let insert_to_dequeue = stat(&mut samples.insert_to_dequeue_us);
    let dequeue_to_loaded = stat(&mut samples.dequeue_to_loaded_us);
    let loaded_to_notified = stat(&mut samples.loaded_to_notified_us);
    let total = stat(&mut samples.total_us);
    let spread = stat(&mut samples.arrival_spread_us);
    datapoint_info!(
        "shred-geyser-latency",
        ("data_sets", count as i64, i64),
        ("untracked_data_sets", untracked as i64, i64),
        ("fetch_to_sigverify_p50_us", fetch_to_sigverify[0], i64),
        ("fetch_to_sigverify_p90_us", fetch_to_sigverify[1], i64),
        ("fetch_to_sigverify_p99_us", fetch_to_sigverify[2], i64),
        ("fetch_to_sigverify_max_us", fetch_to_sigverify[3], i64),
        ("sigverify_to_insert_p50_us", sigverify_to_insert[0], i64),
        ("sigverify_to_insert_p90_us", sigverify_to_insert[1], i64),
        ("sigverify_to_insert_p99_us", sigverify_to_insert[2], i64),
        ("sigverify_to_insert_max_us", sigverify_to_insert[3], i64),
        ("insert_to_dequeue_p50_us", insert_to_dequeue[0], i64),
        ("insert_to_dequeue_p90_us", insert_to_dequeue[1], i64),
        ("insert_to_dequeue_p99_us", insert_to_dequeue[2], i64),
        ("insert_to_dequeue_max_us", insert_to_dequeue[3], i64),
        ("dequeue_to_loaded_p50_us", dequeue_to_loaded[0], i64),
        ("dequeue_to_loaded_p90_us", dequeue_to_loaded[1], i64),
        ("dequeue_to_loaded_p99_us", dequeue_to_loaded[2], i64),
        ("dequeue_to_loaded_max_us", dequeue_to_loaded[3], i64),
        ("loaded_to_notified_p50_us", loaded_to_notified[0], i64),
        ("loaded_to_notified_p90_us", loaded_to_notified[1], i64),
        ("loaded_to_notified_p99_us", loaded_to_notified[2], i64),
        ("loaded_to_notified_max_us", loaded_to_notified[3], i64),
        ("total_p50_us", total[0], i64),
        ("total_p90_us", total[1], i64),
        ("total_p99_us", total[2], i64),
        ("total_max_us", total[3], i64),
        ("arrival_spread_p50_us", spread[0], i64),
        ("arrival_spread_p90_us", spread[1], i64),
        ("arrival_spread_p99_us", spread[2], i64),
        ("arrival_spread_max_us", spread[3], i64),
    );
}

#[cfg(test)]
mod tests {
    use {super::*, std::thread::sleep};

    /// Builds the smallest buffer the tracker parses: a chained merkle data shred header
    /// with the given slot and index.
    fn data_shred(slot: Slot, index: u32) -> Vec<u8> {
        let mut shred = vec![0u8; 88];
        shred[64] = 0x90; // chained merkle data shred variant
        shred[65..73].copy_from_slice(&slot.to_le_bytes());
        shred[73..77].copy_from_slice(&index.to_le_bytes());
        shred
    }

    #[test]
    fn test_disabled_tracks_nothing() {
        let tracker = PipelineLatencyTracker::new(0);
        tracker.mark_fetched(Instant::now(), &data_shred(4, 0));
        assert!(tracker.shreds.is_empty());
    }

    #[test]
    fn test_only_sampled_slots_are_tracked() {
        let tracker = PipelineLatencyTracker::new(2);
        tracker.mark_fetched(Instant::now(), &data_shred(3, 0));
        assert!(tracker.shreds.is_empty());
        tracker.mark_fetched(Instant::now(), &data_shred(4, 0));
        assert_eq!(tracker.shreds.len(), 1);
    }

    #[test]
    fn test_full_data_set_produces_sample_from_last_arriving_shred() {
        let tracker = PipelineLatencyTracker::new(1);
        let slot = 10;
        let shreds: Vec<_> = (0..3).map(|i| data_shred(slot, i)).collect();
        tracker.mark_fetched(Instant::now(), &shreds[0]);
        sleep(Duration::from_millis(5));
        tracker.mark_fetched(Instant::now(), &shreds[1]);
        tracker.mark_fetched(Instant::now(), &shreds[2]);
        tracker.mark_sigverified(shreds.iter().map(|s| s.as_slice()));
        let indices = 0..3;
        tracker.mark_inserted(std::iter::once((slot, &indices)));
        let dequeued = Instant::now();
        tracker.finish(
            slot,
            &indices,
            DeshredStageTimes {
                dequeued,
                entries_loaded: Instant::now(),
            },
        );
        let samples = tracker.samples.lock().unwrap();
        assert_eq!(samples.total_us.len(), 1);
        assert_eq!(samples.untracked_data_sets, 0);
        // Shred 0 arrived >= 5ms before the other two.
        assert!(samples.arrival_spread_us[0] >= 5_000);
        // Latency is measured from the last arriving shred, so it does not
        // include the spread.
        assert!(samples.total_us[0] < samples.arrival_spread_us[0] + 5_000);
        // Entries are cleaned up on completion.
        assert!(tracker.shreds.is_empty());
        assert!(tracker.inserted.is_empty());
    }

    #[test]
    fn test_untracked_data_set_is_counted() {
        let tracker = PipelineLatencyTracker::new(1);
        let now = Instant::now();
        tracker.finish(
            8,
            &(0..2),
            DeshredStageTimes {
                dequeued: now,
                entries_loaded: now,
            },
        );
        assert_eq!(tracker.samples.lock().unwrap().untracked_data_sets, 1);
    }

    #[test]
    fn test_prune_drops_old_slots() {
        let tracker = PipelineLatencyTracker::new(1);
        tracker.mark_fetched(Instant::now(), &data_shred(1, 0));
        tracker.mark_fetched(Instant::now(), &data_shred(1000, 0));
        tracker.prune();
        assert_eq!(tracker.shreds.len(), 1);
        assert!(tracker.shreds.contains_key(&(1000, 0)));
    }

    #[test]
    fn test_percentile() {
        assert_eq!(percentile(&[], 99), 0);
        let v: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&v, 50), 51);
        assert_eq!(percentile(&v, 99), 100);
    }
}

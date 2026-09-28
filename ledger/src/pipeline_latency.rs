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
//! The same tracker also follows sampled slots through replay and the geyser
//! slot/transaction notifications (see [`SlotStage`]) and emits one
//! `slot-geyser-latency` datapoint per slot when it is rooted, with the delays
//! between the stages measured from the arrival of the slot's first shred.
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

/// Per-slot stages, recorded when the geyser plugins have been notified
/// (or, for [`SlotStage::ReplayStart`] and [`SlotStage::Frozen`], when replay
/// reaches that point).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotStage {
    /// `FirstShredReceived` slot status delivered to plugins.
    FirstShredNotified,
    /// `Completed` slot status delivered to plugins.
    Completed,
    /// `CreatedBank` slot status delivered to plugins.
    CreatedBank,
    /// Replay starts replaying the slot from blockstore.
    ReplayStart,
    /// Replay froze the bank.
    Frozen,
    /// `Processed` slot status delivered to plugins.
    Processed,
    /// `Confirmed` slot status delivered to plugins.
    Confirmed,
    /// `Rooted` slot status delivered to plugins. Ends tracking of the slot
    /// and emits the `slot-geyser-latency` datapoint.
    Rooted,
}

#[derive(Clone, Copy, Debug, Default)]
struct SlotTimes {
    first_fetched: Option<Instant>,
    first_shred_notified: Option<Instant>,
    completed: Option<Instant>,
    created_bank: Option<Instant>,
    replay_start: Option<Instant>,
    frozen: Option<Instant>,
    processed: Option<Instant>,
    confirmed: Option<Instant>,
    rooted: Option<Instant>,
    first_tx_notified: Option<Instant>,
    last_tx_notified: Option<Instant>,
    tx_count: u64,
}

/// Delay between two slot stages in microseconds, `-1` if either is missing.
/// Negative differences (the later stage happened first) are reported as 0.
fn delta_us(from: Option<Instant>, to: Option<Instant>) -> i64 {
    match (from, to) {
        (Some(from), Some(to)) => to.saturating_duration_since(from).as_micros() as i64,
        _ => -1,
    }
}

impl SlotTimes {
    fn stage_mut(&mut self, stage: SlotStage) -> &mut Option<Instant> {
        match stage {
            SlotStage::FirstShredNotified => &mut self.first_shred_notified,
            SlotStage::Completed => &mut self.completed,
            SlotStage::CreatedBank => &mut self.created_bank,
            SlotStage::ReplayStart => &mut self.replay_start,
            SlotStage::Frozen => &mut self.frozen,
            SlotStage::Processed => &mut self.processed,
            SlotStage::Confirmed => &mut self.confirmed,
            SlotStage::Rooted => &mut self.rooted,
        }
    }

    /// The reported delays, as (field name, microseconds).
    fn deltas(&self) -> [(&'static str, i64); 15] {
        [
            (
                "first_fetched_to_first_shred_notified_us",
                delta_us(self.first_fetched, self.first_shred_notified),
            ),
            (
                "first_fetched_to_completed_us",
                delta_us(self.first_fetched, self.completed),
            ),
            (
                "first_fetched_to_created_bank_us",
                delta_us(self.first_fetched, self.created_bank),
            ),
            (
                "created_bank_to_replay_start_us",
                delta_us(self.created_bank, self.replay_start),
            ),
            (
                "replay_start_to_first_tx_us",
                delta_us(self.replay_start, self.first_tx_notified),
            ),
            (
                "first_fetched_to_first_tx_us",
                delta_us(self.first_fetched, self.first_tx_notified),
            ),
            (
                "first_fetched_to_last_tx_us",
                delta_us(self.first_fetched, self.last_tx_notified),
            ),
            (
                "completed_to_last_tx_us",
                delta_us(self.completed, self.last_tx_notified),
            ),
            (
                "replay_start_to_frozen_us",
                delta_us(self.replay_start, self.frozen),
            ),
            (
                "frozen_to_last_tx_us",
                delta_us(self.frozen, self.last_tx_notified),
            ),
            (
                "first_fetched_to_processed_us",
                delta_us(self.first_fetched, self.processed),
            ),
            (
                "frozen_to_processed_us",
                delta_us(self.frozen, self.processed),
            ),
            (
                "processed_to_confirmed_us",
                delta_us(self.processed, self.confirmed),
            ),
            (
                "first_fetched_to_confirmed_us",
                delta_us(self.first_fetched, self.confirmed),
            ),
            (
                "confirmed_to_rooted_us",
                delta_us(self.confirmed, self.rooted),
            ),
        ]
    }

    fn report(&self, slot: Slot) {
        let d = self.deltas();
        datapoint_info!(
            "slot-geyser-latency",
            ("slot", slot as i64, i64),
            ("tx_count", self.tx_count as i64, i64),
            (d[0].0, d[0].1, i64),
            (d[1].0, d[1].1, i64),
            (d[2].0, d[2].1, i64),
            (d[3].0, d[3].1, i64),
            (d[4].0, d[4].1, i64),
            (d[5].0, d[5].1, i64),
            (d[6].0, d[6].1, i64),
            (d[7].0, d[7].1, i64),
            (d[8].0, d[8].1, i64),
            (d[9].0, d[9].1, i64),
            (d[10].0, d[10].1, i64),
            (d[11].0, d[11].1, i64),
            (d[12].0, d[12].1, i64),
            (d[13].0, d[13].1, i64),
            (d[14].0, d[14].1, i64),
        );
    }
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
    /// Last slot for which the first fetch time was recorded; avoids a map
    /// lookup for every shred.
    last_first_fetch_slot: AtomicU64,
    shreds: DashMap<(Slot, u32), ShredTimes>,
    /// Keyed by (slot, first shred index of the data set).
    inserted: DashMap<(Slot, u32), Instant>,
    slots: DashMap<Slot, SlotTimes>,
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
            last_first_fetch_slot: AtomicU64::default(),
            shreds: DashMap::default(),
            inserted: DashMap::default(),
            slots: DashMap::default(),
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
        if self.last_first_fetch_slot.load(Ordering::Relaxed) != key.0 {
            self.last_first_fetch_slot.store(key.0, Ordering::Relaxed);
            self.slots
                .entry(key.0)
                .or_default()
                .first_fetched
                .get_or_insert(now);
        }
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

    /// Records that `slot` reached `stage`. Keeps the first timestamp if the
    /// stage is recorded more than once. [`SlotStage::Rooted`] ends tracking
    /// of the slot and reports it.
    pub fn mark_slot(&self, slot: Slot, stage: SlotStage) {
        if !self.is_sampled(slot) {
            return;
        }
        let now = Instant::now();
        self.max_fetched_slot.fetch_max(slot, Ordering::Relaxed);
        if stage == SlotStage::Rooted {
            if let Some((_, mut times)) = self.slots.remove(&slot) {
                times.rooted = Some(now);
                times.report(slot);
            }
            // Slots are pruned from the report path, which the deshred
            // notifier may never drive (e.g. no deshred plugin).
            self.maybe_report(now);
        } else {
            self.slots
                .entry(slot)
                .or_default()
                .stage_mut(stage)
                .get_or_insert(now);
        }
    }

    /// Records that a transaction of `slot` was delivered to the transaction
    /// plugins.
    pub fn mark_tx_notified(&self, slot: Slot) {
        if !self.is_sampled(slot) {
            return;
        }
        let now = Instant::now();
        let mut times = self.slots.entry(slot).or_default();
        times.first_tx_notified.get_or_insert(now);
        times.last_tx_notified = Some(now);
        times.tx_count += 1;
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
        self.slots.retain(|slot, _| *slot >= min_slot);
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
    fn test_slot_stages_are_tracked_and_rooted_slot_is_removed() {
        let tracker = PipelineLatencyTracker::new(1);
        tracker.mark_fetched(Instant::now(), &data_shred(6, 0));
        assert!(tracker.slots.get(&6).unwrap().first_fetched.is_some());
        tracker.mark_slot(6, SlotStage::Completed);
        tracker.mark_slot(6, SlotStage::CreatedBank);
        tracker.mark_tx_notified(6);
        tracker.mark_tx_notified(6);
        {
            let times = tracker.slots.get(&6).unwrap();
            assert!(times.completed.is_some());
            assert!(times.created_bank.is_some());
            assert_eq!(times.tx_count, 2);
            assert!(times.last_tx_notified >= times.first_tx_notified);
        }
        tracker.mark_slot(6, SlotStage::Rooted);
        assert!(tracker.slots.get(&6).is_none());
    }

    #[test]
    fn test_first_stage_timestamp_is_kept() {
        let tracker = PipelineLatencyTracker::new(1);
        tracker.mark_slot(2, SlotStage::Processed);
        let first = tracker.slots.get(&2).unwrap().processed.unwrap();
        sleep(Duration::from_millis(2));
        tracker.mark_slot(2, SlotStage::Processed);
        assert_eq!(tracker.slots.get(&2).unwrap().processed.unwrap(), first);
    }

    #[test]
    fn test_unsampled_slots_are_ignored() {
        let tracker = PipelineLatencyTracker::new(2);
        tracker.mark_slot(3, SlotStage::Completed);
        tracker.mark_tx_notified(3);
        assert!(tracker.slots.is_empty());
    }

    #[test]
    fn test_slot_deltas() {
        let base = Instant::now();
        let at = |ms| Some(base + Duration::from_millis(ms));
        let times = SlotTimes {
            first_fetched: at(0),
            completed: at(100),
            created_bank: at(10),
            replay_start: at(20),
            first_tx_notified: at(30),
            last_tx_notified: at(140),
            // frozen after the last tx: frozen_to_last_tx clamps to 0.
            frozen: at(150),
            ..Default::default()
        };
        let deltas: std::collections::HashMap<_, _> = times.deltas().into_iter().collect();
        assert_eq!(deltas["first_fetched_to_completed_us"], 100_000);
        assert_eq!(deltas["created_bank_to_replay_start_us"], 10_000);
        assert_eq!(deltas["first_fetched_to_first_tx_us"], 30_000);
        assert_eq!(deltas["completed_to_last_tx_us"], 40_000);
        assert_eq!(deltas["frozen_to_last_tx_us"], 0);
        // Missing stages are reported as -1.
        assert_eq!(deltas["first_fetched_to_processed_us"], -1);
        assert_eq!(deltas["confirmed_to_rooted_us"], -1);
    }

    #[test]
    fn test_prune_drops_old_slots_from_slot_map() {
        let tracker = PipelineLatencyTracker::new(1);
        tracker.mark_slot(1, SlotStage::Completed);
        tracker.mark_slot(1000, SlotStage::Completed);
        tracker.prune();
        assert_eq!(tracker.slots.len(), 1);
        assert!(tracker.slots.contains_key(&1000));
    }

    #[test]
    fn test_percentile() {
        assert_eq!(percentile(&[], 99), 0);
        let v: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&v, 50), 51);
        assert_eq!(percentile(&v, 99), 100);
    }
}

//! Cross-thread tracking for the two `agave_end_to_end_duration_us` spans:
//! `deshred` (first shred received for a `(slot, fec_set_index)` data set ->
//! its deshred transaction notified) and `executed_tx` (first shred received
//! for a slot -> each transaction notified for that slot).
//!
//! Slot is passed as a plain `u64` rather than `solana_clock::Slot` so this
//! crate doesn't need to depend on it.

use {
    crate::pipeline_metrics::END_TO_END_DURATION_US,
    dashmap::DashMap,
    std::{
        sync::{
            LazyLock,
            atomic::{AtomicU64, Ordering},
        },
        time::Instant,
    },
};

/// Tracked start timestamps for slots more than this many slots behind the
/// highest slot seen are dropped, so forks/dead slots don't leak memory.
const MAX_TRACKED_SLOT_AGE: u64 = 64;

pub struct DeshredLatencyTracker {
    started: DashMap<(u64, u32), Instant>,
    max_slot_seen: AtomicU64,
}

impl DeshredLatencyTracker {
    fn new() -> Self {
        Self {
            started: DashMap::new(),
            max_slot_seen: AtomicU64::new(0),
        }
    }

    /// Record the first-shred-seen instant for a data set. A no-op if a
    /// start was already recorded for this `(slot, fec_set_index)`.
    pub fn mark_started(&self, slot: u64, fec_set_index: u32) {
        self.started
            .entry((slot, fec_set_index))
            .or_insert_with(Instant::now);
        self.maybe_prune(slot);
    }

    /// Record that the data set's deshred transaction was notified,
    /// observing the end-to-end duration if a start was recorded for it.
    pub fn mark_notified(&self, slot: u64, fec_set_index: u32) {
        if let Some((_, start)) = self.started.remove(&(slot, fec_set_index)) {
            END_TO_END_DURATION_US
                .with_label_values(&["deshred"])
                .observe(start.elapsed().as_micros() as f64);
        }
    }

    fn maybe_prune(&self, slot: u64) {
        let previous_max = self.max_slot_seen.fetch_max(slot, Ordering::Relaxed);
        if slot <= previous_max || slot < MAX_TRACKED_SLOT_AGE {
            return;
        }
        let cutoff = slot - MAX_TRACKED_SLOT_AGE;
        self.started.retain(|(entry_slot, _), _| *entry_slot >= cutoff);
    }
}

pub static DESHRED_LATENCY: LazyLock<DeshredLatencyTracker> =
    LazyLock::new(DeshredLatencyTracker::new);

pub struct ExecutedTxLatencyTracker {
    started: DashMap<u64, Instant>,
    max_slot_seen: AtomicU64,
}

impl ExecutedTxLatencyTracker {
    fn new() -> Self {
        Self {
            started: DashMap::new(),
            max_slot_seen: AtomicU64::new(0),
        }
    }

    /// Record the first-shred-seen instant for a slot. A no-op if a start
    /// was already recorded for this slot.
    pub fn mark_slot_started(&self, slot: u64) {
        self.started.entry(slot).or_insert_with(Instant::now);
        self.maybe_prune(slot);
    }

    /// Observe how long after the slot's first shred this transaction was
    /// notified. Called once per notified transaction (not just the last),
    /// so the histogram reflects the full per-transaction distribution.
    pub fn mark_tx_notified(&self, slot: u64) {
        if let Some(start) = self.started.get(&slot) {
            END_TO_END_DURATION_US
                .with_label_values(&["executed_tx"])
                .observe(start.elapsed().as_micros() as f64);
        }
    }

    fn maybe_prune(&self, slot: u64) {
        let previous_max = self.max_slot_seen.fetch_max(slot, Ordering::Relaxed);
        if slot <= previous_max || slot < MAX_TRACKED_SLOT_AGE {
            return;
        }
        let cutoff = slot - MAX_TRACKED_SLOT_AGE;
        self.started.retain(|entry_slot, _| *entry_slot >= cutoff);
    }
}

pub static EXECUTED_TX_LATENCY: LazyLock<ExecutedTxLatencyTracker> =
    LazyLock::new(ExecutedTxLatencyTracker::new);

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_deshred_tracker_observes_only_after_start() {
        // mark_notified with no prior mark_started must not panic and must
        // not observe anything.
        DESHRED_LATENCY.mark_notified(999_999, 0);

        DESHRED_LATENCY.mark_started(1, 7);
        DESHRED_LATENCY.mark_notified(1, 7);
        // Second notify for the same key is a no-op (already removed).
        DESHRED_LATENCY.mark_notified(1, 7);
    }

    #[test]
    fn test_executed_tx_tracker_multiple_notifies() {
        EXECUTED_TX_LATENCY.mark_slot_started(1);
        EXECUTED_TX_LATENCY.mark_tx_notified(1);
        EXECUTED_TX_LATENCY.mark_tx_notified(1);
    }

    #[test]
    fn test_pruning_drops_old_slots() {
        let tracker = DeshredLatencyTracker::new();
        tracker.mark_started(1, 0);
        tracker.mark_started(1_000, 0);
        assert!(!tracker.started.contains_key(&(1, 0)));
        assert!(tracker.started.contains_key(&(1_000, 0)));
    }
}

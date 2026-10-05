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
    /// Also records, via `DESHRED_TRACKING_TOTAL`, whether a matching start
    /// was actually found -- see that metric's doc comment for why this
    /// isn't guaranteed on every call.
    pub fn mark_notified(&self, slot: u64, fec_set_index: u32) {
        if let Some((_, start)) = self.started.remove(&(slot, fec_set_index)) {
            END_TO_END_DURATION_US
                .with_label_values(&["deshred"])
                .observe(start.elapsed().as_micros() as f64);
            crate::pipeline_metrics::DESHRED_TRACKING_TOTAL
                .with_label_values(&["tracked"])
                .inc();
        } else {
            crate::pipeline_metrics::DESHRED_TRACKING_TOTAL
                .with_label_values(&["untracked"])
                .inc();
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

    /// Observes `first_shred_to_created_bank` ("replay wake-up latency" --
    /// how long between this validator first fetching a shred for the slot
    /// and replay creating a bank for it) if a start was recorded. Does not
    /// remove the entry -- `mark_tx_notified` still needs it afterward.
    /// Added after `executed_tx` (first-shred -> commit) turned out much
    /// larger than `created_bank_to_frozen` (created-bank -> freeze,
    /// independently measured as fast): since `notify_transaction` fires at
    /// commit with no dependency on voting, the only place that gap could be
    /// hiding is between shred arrival and replay actually starting on this
    /// slot, which until now was never directly measured.
    pub fn mark_bank_created(&self, slot: u64) {
        if let Some(start) = self.started.get(&slot) {
            crate::pipeline_metrics::SLOT_CONFIRMATION_DURATION_US
                .with_label_values(&["first_shred_to_created_bank"])
                .observe(start.elapsed().as_micros() as f64);
        }
    }

    /// Observes `first_shred_to_frozen` -- a DIRECT measurement of first
    /// shred fetched to bank freeze (all transactions in the slot executed
    /// and committed), as opposed to inferring it by subtracting
    /// `created_bank_to_confirmed` minus `frozen_to_confirmed` (two
    /// independently-computed percentiles, which is not valid: p90(A) -
    /// p90(B) != p90(A - B) in general). Added after `first_shred_to_created_bank`
    /// came back small, which combined with the (invalid) subtraction implied
    /// the whole slot finishes within ~40ms of the first shred -- hard to
    /// square with `executed_tx` p90 being ~400ms, since no transaction can
    /// be notified after its slot freezes. This measures the true value
    /// directly instead of composing it from two other metrics.
    pub fn mark_bank_frozen(&self, slot: u64) {
        if let Some(start) = self.started.get(&slot) {
            crate::pipeline_metrics::SLOT_CONFIRMATION_DURATION_US
                .with_label_values(&["first_shred_to_frozen"])
                .observe(start.elapsed().as_micros() as f64);
        }
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

/// Per-slot timestamps for the slot-status lifecycle (`CreatedBank` ->
/// `Processed` (bank freeze, i.e. transaction processing finished) ->
/// `Confirmed` (optimistic confirmation via votes) -> ... ). Votes are
/// counted by a separate subsystem (`OptimisticallyConfirmedBankTracker`)
/// that runs independently of transaction execution, so `Confirmed` timing is
/// *not* just downstream replay latency -- it's primarily governed by how
/// long it takes the cluster's votes to reach this validator and accumulate
/// past the optimistic-confirmation threshold.
///
/// Timestamps are taken at the top of
/// `geyser-plugin-manager/src/slot_status_notifier.rs::notify_bank_status`,
/// i.e. just before that status is reported to Geyser plugins -- this is a
/// close approximation of, but not exactly, the moment the underlying event
/// happened (there's a small channel hop between the actual bank-freeze/
/// vote-threshold event and this notifier being invoked, analogous to the
/// channel hops already measured elsewhere in this pipeline and found
/// negligible).
#[derive(Default, Clone, Copy)]
struct SlotLifecycleTimestamps {
    created_bank: Option<Instant>,
    frozen: Option<Instant>,
}

pub struct SlotConfirmationLatencyTracker {
    slots: DashMap<u64, SlotLifecycleTimestamps>,
    max_slot_seen: AtomicU64,
}

impl SlotConfirmationLatencyTracker {
    fn new() -> Self {
        Self {
            slots: DashMap::new(),
            max_slot_seen: AtomicU64::new(0),
        }
    }

    pub fn mark_created_bank(&self, slot: u64) {
        self.slots
            .entry(slot)
            .or_default()
            .created_bank
            .get_or_insert_with(Instant::now);
        self.maybe_prune(slot);
    }

    /// Records the freeze timestamp and, if `created_bank` was already
    /// recorded for this slot, directly observes `created_bank_to_frozen`
    /// as the true `Instant`-to-`Instant` delta between the two -- NOT by
    /// subtracting two independently-computed percentiles of other metrics
    /// (that approach was tried and gave a wildly different, wrong answer
    /// for this same quantity; see the doc comment on
    /// `ExecutedTxLatencyTracker::mark_bank_frozen`).
    pub fn mark_frozen(&self, slot: u64) {
        let now = Instant::now();
        let mut entry = self.slots.entry(slot).or_default();
        let frozen = *entry.frozen.get_or_insert(now);
        if let Some(created_bank) = entry.created_bank {
            crate::pipeline_metrics::SLOT_CONFIRMATION_DURATION_US
                .with_label_values(&["created_bank_to_frozen"])
                .observe(frozen.saturating_duration_since(created_bank).as_micros() as f64);
        }
        drop(entry);
        self.maybe_prune(slot);
    }

    /// Observes `created_bank_to_confirmed` ("time taken for votes to
    /// confirm a slot", from when this validator started tracking it) and
    /// `frozen_to_confirmed` ("time difference of bank freeze, i.e.
    /// transaction processing finished, to slot marked confirmed") for
    /// whichever start timestamps were actually recorded.
    pub fn mark_confirmed(&self, slot: u64) {
        let Some(timestamps) = self.slots.get(&slot).map(|entry| *entry) else {
            return;
        };
        if let Some(created_bank) = timestamps.created_bank {
            crate::pipeline_metrics::SLOT_CONFIRMATION_DURATION_US
                .with_label_values(&["created_bank_to_confirmed"])
                .observe(created_bank.elapsed().as_micros() as f64);
        }
        if let Some(frozen) = timestamps.frozen {
            crate::pipeline_metrics::SLOT_CONFIRMATION_DURATION_US
                .with_label_values(&["frozen_to_confirmed"])
                .observe(frozen.elapsed().as_micros() as f64);
        }
    }

    fn maybe_prune(&self, slot: u64) {
        let previous_max = self.max_slot_seen.fetch_max(slot, Ordering::Relaxed);
        if slot <= previous_max || slot < MAX_TRACKED_SLOT_AGE {
            return;
        }
        let cutoff = slot - MAX_TRACKED_SLOT_AGE;
        self.slots.retain(|entry_slot, _| *entry_slot >= cutoff);
    }
}

pub static SLOT_CONFIRMATION_LATENCY: LazyLock<SlotConfirmationLatencyTracker> =
    LazyLock::new(SlotConfirmationLatencyTracker::new);

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
    fn test_executed_tx_tracker_bank_created_does_not_remove_start() {
        let tracker = ExecutedTxLatencyTracker::new();
        tracker.mark_slot_started(9);
        tracker.mark_bank_created(9);
        // mark_tx_notified must still find the start timestamp afterward.
        assert!(tracker.started.contains_key(&9));
        tracker.mark_tx_notified(9);
    }

    #[test]
    fn test_executed_tx_tracker_bank_created_with_no_start_does_not_panic() {
        let tracker = ExecutedTxLatencyTracker::new();
        tracker.mark_bank_created(999_997);
    }

    #[test]
    fn test_executed_tx_tracker_bank_frozen_does_not_remove_start() {
        let tracker = ExecutedTxLatencyTracker::new();
        tracker.mark_slot_started(10);
        tracker.mark_bank_frozen(10);
        assert!(tracker.started.contains_key(&10));
        tracker.mark_tx_notified(10);
    }

    #[test]
    fn test_executed_tx_tracker_bank_frozen_with_no_start_does_not_panic() {
        let tracker = ExecutedTxLatencyTracker::new();
        tracker.mark_bank_frozen(999_996);
    }

    #[test]
    fn test_slot_confirmation_tracker_no_prior_marks_does_not_panic() {
        // mark_confirmed with no prior created_bank/frozen marks must not panic
        // and must not observe anything.
        SLOT_CONFIRMATION_LATENCY.mark_confirmed(999_998);
    }

    #[test]
    fn test_slot_confirmation_tracker_partial_marks() {
        let tracker = SlotConfirmationLatencyTracker::new();
        // Only created_bank recorded, no frozen -- confirmed should still
        // observe created_bank_to_confirmed without panicking on the missing
        // frozen timestamp.
        tracker.mark_created_bank(2);
        tracker.mark_confirmed(2);
    }

    #[test]
    fn test_slot_confirmation_tracker_both_marks() {
        let tracker = SlotConfirmationLatencyTracker::new();
        tracker.mark_created_bank(3);
        // Exercises the new created_bank_to_frozen direct observation inside
        // mark_frozen (created_bank was already recorded above).
        tracker.mark_frozen(3);
        tracker.mark_confirmed(3);
    }

    #[test]
    fn test_slot_confirmation_tracker_frozen_before_created_bank_does_not_panic() {
        let tracker = SlotConfirmationLatencyTracker::new();
        // No created_bank recorded yet -- mark_frozen must not observe
        // created_bank_to_frozen and must not panic.
        tracker.mark_frozen(4);
        tracker.mark_created_bank(4);
        tracker.mark_confirmed(4);
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

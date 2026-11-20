//! Cross-thread tracking for the two `agave_end_to_end_duration_us` spans:
//! `deshred` (first shred received for a `(slot, fec_set_index)` data set ->
//! its deshred transaction notified) and `executed_tx` (first shred received
//! for a slot -> each transaction notified for that slot).
//!
//! Slot is passed as a plain `u64` rather than `solana_clock::Slot` so this
//! crate doesn't need to depend on it.

use {
    crate::pipeline_metrics::{END_TO_END_DURATION_US, TX_PIPELINE_SEGMENT_DURATION_US},
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
    /// When replay froze each slot's bank, taken on the replay thread (the
    /// `Processed` slot status reaches the notifier later, through
    /// `OptimisticallyConfirmedBankTracker`'s channel, so it can't be used to
    /// split `first_shred_to_created_bank` against the replay-thread
    /// `CreatedBank` timestamp).
    replay_frozen: DashMap<u64, Instant>,
    max_slot_seen: AtomicU64,
}

impl ExecutedTxLatencyTracker {
    fn new() -> Self {
        Self {
            started: DashMap::new(),
            replay_frozen: DashMap::new(),
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
    ///
    /// When `parent`'s replay freeze was recorded (`mark_replay_frozen`), also
    /// splits that span at the later of (first shred, parent frozen), since a
    /// bank is only created once its parent is frozen:
    /// - `first_shred_to_parent_frozen`: waiting for the parent to freeze (0 if
    ///   it froze before this slot's first shred arrived);
    /// - `parent_frozen_to_created_bank`: the rest -- replay loop wake-up and
    ///   discovering the child through the blockstore (`next_slots`, `meta`).
    ///
    /// The two sum to `first_shred_to_created_bank` for the same slot.
    pub fn mark_bank_created(&self, slot: u64, parent: Option<u64>) {
        let now = Instant::now();
        let Some(start) = self.started.get(&slot).map(|start| *start) else {
            return;
        };
        let slot_confirmation_us = &crate::pipeline_metrics::SLOT_CONFIRMATION_DURATION_US;
        slot_confirmation_us
            .with_label_values(&["first_shred_to_created_bank"])
            .observe(now.saturating_duration_since(start).as_micros() as f64);
        let parent_frozen = parent.and_then(|parent| {
            self.replay_frozen
                .get(&parent)
                .map(|parent_frozen| *parent_frozen)
        });
        if let Some(parent_frozen) = parent_frozen {
            let ready = start.max(parent_frozen);
            slot_confirmation_us
                .with_label_values(&["first_shred_to_parent_frozen"])
                .observe(ready.saturating_duration_since(start).as_micros() as f64);
            slot_confirmation_us
                .with_label_values(&["parent_frozen_to_created_bank"])
                .observe(now.saturating_duration_since(ready).as_micros() as f64);
        }
    }

    /// Records when replay froze `slot`'s bank. Called on the replay thread,
    /// right after the freeze; see `replay_frozen`.
    pub fn mark_replay_frozen(&self, slot: u64) {
        self.replay_frozen.entry(slot).or_insert_with(Instant::now);
        self.maybe_prune(slot);
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
    ///
    /// NOTE: this measures position-within-slot, not true pipeline latency --
    /// a transaction late in a large/slow-replaying slot will show a large
    /// value here even if the pipeline processed *it* (from its own shreds)
    /// quickly. For true per-transaction pipeline latency, see
    /// `TX_PIPELINE_LATENCY` / `path="tx_pipeline"` below, which starts the
    /// clock at that transaction's own data set's first shred instead.
    pub fn mark_tx_notified(&self, slot: u64) {
        if let Some(start) = self.started.get(&slot) {
            END_TO_END_DURATION_US
                .with_label_values(&["executed_tx"])
                .observe(start.elapsed().as_micros() as f64);
        }
    }

    /// Observes `agave_shred_arrival_spread_us` -- time from this slot's
    /// first shred fetched to its LAST shred fetched (the shred carrying the
    /// `LAST_SHRED_IN_SLOT` flag) -- if a start was recorded. Does not
    /// remove the entry (`mark_tx_notified` may still need it).
    pub fn mark_last_shred(&self, slot: u64) {
        if let Some(start) = self.started.get(&slot) {
            crate::pipeline_metrics::SHRED_ARRIVAL_SPREAD_US
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
        self.replay_frozen.retain(|entry_slot, _| *entry_slot >= cutoff);
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

/// True per-transaction pipeline latency: from the first shred of the DATA
/// SET containing this transaction being fetched, to this transaction being
/// sent over the Geyser channel -- as opposed to `ExecutedTxLatencyTracker`
/// (`path="executed_tx"`), which measures from the SLOT's first shred and
/// therefore conflates true pipeline latency with how late in the slot a
/// transaction happens to land. This tracker answers "how fast is the
/// pipeline itself," independent of slot position -- added per explicit
/// request after the `executed_tx`/slot-replay-time investigation
/// (see docs/perf/shred-to-geyser-prometheus-plan.md) made clear the two
/// questions need separate metrics.
///
/// Deliberately keeps its OWN `(slot, fec_set_index) -> Instant` map rather
/// than reading `DeshredLatencyTracker`'s: that one is raced-and-removed by
/// the separate, concurrent `CompletedDataSetsService` consumer of the same
/// underlying shred data, so it can't be read reliably from here (the
/// replay path, a different, independent consumer of the same shreds).
pub struct TxPipelineLatencyTracker {
    data_set_started: DashMap<(u64, u32), Instant>,
    /// `(slot, data set start index)` -> instant the decoded data set was
    /// sent to ReplayStage. Only populated on the live-replay path.
    data_set_ready: DashMap<(u64, u32), Instant>,
    /// Signature bytes -> checkpoints. Slot is kept alongside purely for
    /// age-based pruning, since this map isn't keyed by slot directly like
    /// the others in this file.
    tx_started: DashMap<[u8; 64], TxCheckpoints>,
    max_slot_seen: AtomicU64,
}

struct TxCheckpoints {
    slot: u64,
    start: Instant,
    ready: Option<Instant>,
    picked_up: Instant,
    /// Replay thread handed the transaction's task to the unified scheduler.
    scheduled: Option<Instant>,
    /// A unified-scheduler handler thread started executing it.
    exec_start: Option<Instant>,
    /// SVM load-and-execute finished (before the pre-commit check and commit).
    executed: Option<Instant>,
    enqueued: Option<Instant>,
    dequeued: Option<Instant>,
}

impl TxPipelineLatencyTracker {
    fn new() -> Self {
        Self {
            data_set_started: DashMap::new(),
            data_set_ready: DashMap::new(),
            tx_started: DashMap::new(),
            max_slot_seen: AtomicU64::new(0),
        }
    }

    /// Mirrors `DeshredLatencyTracker::mark_started` -- records the
    /// first-shred-seen instant for a data set, independently.
    pub fn mark_data_set_started(&self, slot: u64, fec_set_index: u32) {
        self.data_set_started
            .entry((slot, fec_set_index))
            .or_insert_with(Instant::now);
        self.maybe_prune(slot);
    }

    /// Records that the data set starting at `start_index` was decoded and
    /// handed to ReplayStage (end of the `fetch_to_data_set_ready` segment).
    pub fn mark_data_set_ready(&self, slot: u64, start_index: u32) {
        self.data_set_ready
            .entry((slot, start_index))
            .or_insert_with(Instant::now);
    }

    /// Splits the data set's `fetch_to_data_set_ready` span at
    /// `completing_batch_received` (when `window_service` dequeued the shred
    /// batch whose insert completed it) and records how its data shreds
    /// arrived (`origin`: `turbine`, `repair` or `recovered`); see
    /// `DATA_SET_COMPLETION_DURATION_US`. Call right after
    /// `mark_data_set_ready` for the same data set: the end of the span is
    /// taken here, a few nanoseconds after that mark. The completion is
    /// counted even when no start was recorded for the data set.
    pub fn mark_data_set_completed(
        &self,
        slot: u64,
        start_index: u32,
        completing_batch_received: Instant,
        origin: &str,
    ) {
        let ready = Instant::now();
        crate::pipeline_metrics::DATA_SET_COMPLETIONS_TOTAL
            .with_label_values(&[origin])
            .inc();
        let Some(start) = self
            .data_set_started
            .get(&(slot, start_index))
            .map(|start| *start)
        else {
            return;
        };
        let durations = &crate::pipeline_metrics::DATA_SET_COMPLETION_DURATION_US;
        durations
            .with_label_values(&["first_fetch_to_completing_recv", origin])
            .observe(
                completing_batch_received
                    .saturating_duration_since(start)
                    .as_micros() as f64,
            );
        durations
            .with_label_values(&["completing_recv_to_ready", origin])
            .observe(
                ready
                    .saturating_duration_since(completing_batch_received)
                    .as_micros() as f64,
            );
    }

    /// Called once per transaction when replay processes the data set
    /// containing it (before scheduling execution), recording that
    /// transaction's true pipeline start time -- its own data set's first
    /// shred, not the slot's. A no-op if no start was recorded for that data
    /// set (e.g. the data set arrived via repair/leader-local path with no
    /// fetch-stage timestamp).
    pub fn mark_tx_data_set(&self, signature: [u8; 64], slot: u64, fec_set_index: u32) {
        if let Some(start) = self.data_set_started.get(&(slot, fec_set_index)) {
            let ready = self
                .data_set_ready
                .get(&(slot, fec_set_index))
                .map(|ready| *ready);
            self.tx_started.insert(
                signature,
                TxCheckpoints {
                    slot,
                    start: *start,
                    ready,
                    picked_up: Instant::now(),
                    scheduled: None,
                    exec_start: None,
                    executed: None,
                    enqueued: None,
                    dequeued: None,
                },
            );
        }
    }

    /// Called on the replay thread right before the transaction's task is
    /// sent to the unified scheduler (`PooledScheduler::schedule_execution`).
    /// A no-op for untracked signatures.
    pub fn mark_tx_scheduled(&self, signature: [u8; 64]) {
        if let Some(mut checkpoints) = self.tx_started.get_mut(&signature) {
            checkpoints.scheduled = Some(Instant::now());
        }
    }

    /// Called on a unified-scheduler handler thread right before it executes
    /// the transaction. `blocked` is whether the scheduler had buffered the
    /// task behind an account-lock conflict (it arrived on the handlers'
    /// blocked-task channel) rather than dispatching it as soon as it was
    /// scheduled. Also observes `SCHEDULER_QUEUE_WAIT_DURATION_US` for it when
    /// `mark_tx_scheduled` was recorded. A no-op for untracked signatures.
    pub fn mark_tx_exec_started(&self, signature: [u8; 64], blocked: bool) {
        let Some(mut checkpoints) = self.tx_started.get_mut(&signature) else {
            return;
        };
        let now = Instant::now();
        checkpoints.exec_start = Some(now);
        let scheduled = checkpoints.scheduled;
        drop(checkpoints);
        if let Some(scheduled) = scheduled {
            crate::pipeline_metrics::SCHEDULER_QUEUE_WAIT_DURATION_US
                .with_label_values(&[if blocked { "blocked" } else { "unblocked" }])
                .observe(now.saturating_duration_since(scheduled).as_micros() as f64);
        }
    }

    /// Called right after the SVM finished loading and executing the batch
    /// holding this transaction (`executed_at`), before its commit. A no-op
    /// for untracked signatures.
    pub fn mark_tx_executed(&self, signature: [u8; 64], executed_at: Instant) {
        if let Some(mut checkpoints) = self.tx_started.get_mut(&signature) {
            checkpoints.executed = Some(executed_at);
        }
    }

    /// Called by the tx-status service when it dequeues the batch holding
    /// this transaction. `enqueued_at` is when the batch was committed and
    /// queued. A no-op for untracked signatures.
    pub fn mark_tx_status_received(&self, signature: [u8; 64], enqueued_at: Instant) {
        if let Some(mut checkpoints) = self.tx_started.get_mut(&signature) {
            checkpoints.enqueued = Some(enqueued_at);
            checkpoints.dequeued = Some(Instant::now());
        }
    }

    /// Observes `agave_end_to_end_duration_us{path="tx_pipeline"}` if a
    /// start was recorded for this signature, plus the per-segment
    /// breakdown when the `ready`, `enqueued` and `dequeued` checkpoints were
    /// recorded, and removes the entry. The replay part (pickup -> committed)
    /// is split into four segments when `scheduled`, `exec_start` and
    /// `executed` were all recorded; otherwise it is observed whole as
    /// `replay_pickup_to_committed`, so a missing stamp shows up as that
    /// segment's count instead of silently dropping the breakdown. Either way
    /// the segments observed for one transaction sum to its `tx_pipeline`.
    /// A transaction that never reaches this (e.g. dropped before commit) is
    /// cleaned up later by age-based pruning instead, bounding memory.
    pub fn mark_tx_notified(&self, signature: [u8; 64]) {
        let Some((_, checkpoints)) = self.tx_started.remove(&signature) else {
            return;
        };
        let now = Instant::now();
        END_TO_END_DURATION_US
            .with_label_values(&["tx_pipeline"])
            .observe(now.saturating_duration_since(checkpoints.start).as_micros() as f64);
        let (Some(ready), Some(enqueued), Some(dequeued)) =
            (checkpoints.ready, checkpoints.enqueued, checkpoints.dequeued)
        else {
            return;
        };
        let observe = |segment: &str, from: Instant, to: Instant| {
            TX_PIPELINE_SEGMENT_DURATION_US
                .with_label_values(&[segment])
                .observe(to.saturating_duration_since(from).as_micros() as f64);
        };
        let picked_up = checkpoints.picked_up;
        observe("fetch_to_data_set_ready", checkpoints.start, ready);
        observe("data_set_ready_to_replay_pickup", ready, picked_up);
        if let (Some(scheduled), Some(exec_start), Some(executed)) = (
            checkpoints.scheduled,
            checkpoints.exec_start,
            checkpoints.executed,
        ) {
            observe("replay_pickup_to_scheduled", picked_up, scheduled);
            observe("scheduled_to_exec_start", scheduled, exec_start);
            observe("exec_start_to_executed", exec_start, executed);
            observe("executed_to_committed", executed, enqueued);
        } else {
            observe("replay_pickup_to_committed", picked_up, enqueued);
        }
        observe("commit_to_status_dequeued", enqueued, dequeued);
        observe("status_dequeued_to_notified", dequeued, now);
    }

    fn maybe_prune(&self, slot: u64) {
        let previous_max = self.max_slot_seen.fetch_max(slot, Ordering::Relaxed);
        if slot <= previous_max || slot < MAX_TRACKED_SLOT_AGE {
            return;
        }
        let cutoff = slot - MAX_TRACKED_SLOT_AGE;
        self.data_set_started
            .retain(|(entry_slot, _), _| *entry_slot >= cutoff);
        self.data_set_ready
            .retain(|(entry_slot, _), _| *entry_slot >= cutoff);
        self.tx_started
            .retain(|_, checkpoints| checkpoints.slot >= cutoff);
    }
}

pub static TX_PIPELINE_LATENCY: LazyLock<TxPipelineLatencyTracker> =
    LazyLock::new(TxPipelineLatencyTracker::new);

#[cfg(test)]
mod test {
    use {super::*, std::sync::Mutex};

    /// Serializes the tests that observe the global
    /// `first_shred_to_parent_frozen` / `parent_frozen_to_created_bank`
    /// histograms, since one of them counts the samples it added.
    static PARENT_FROZEN_SPLIT_LOCK: Mutex<()> = Mutex::new(());

    /// Serializes the tests that count samples they add to the global
    /// `TX_PIPELINE_SEGMENT_DURATION_US` / `SCHEDULER_QUEUE_WAIT_DURATION_US`.
    static TX_PIPELINE_SEGMENT_LOCK: Mutex<()> = Mutex::new(());

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
        tracker.mark_bank_created(9, None);
        // mark_tx_notified must still find the start timestamp afterward.
        assert!(tracker.started.contains_key(&9));
        tracker.mark_tx_notified(9);
    }

    #[test]
    fn test_executed_tx_tracker_bank_created_with_no_start_does_not_panic() {
        let tracker = ExecutedTxLatencyTracker::new();
        tracker.mark_bank_created(999_997, Some(999_996));
    }

    #[test]
    fn test_bank_created_split_sums_to_total() {
        use std::{thread::sleep, time::Duration};
        let _lock = PARENT_FROZEN_SPLIT_LOCK.lock().unwrap();
        let read = |name: &str| {
            let h = crate::pipeline_metrics::SLOT_CONFIRMATION_DURATION_US.with_label_values(&[name]);
            (h.get_sample_count(), h.get_sample_sum())
        };
        let split = ["first_shred_to_parent_frozen", "parent_frozen_to_created_bank"];
        let before = split.map(read);

        // Child's first shred arrives, then the parent freezes, then the child bank is created.
        let tracker = ExecutedTxLatencyTracker::new();
        let begin = Instant::now();
        tracker.mark_slot_started(21);
        sleep(Duration::from_millis(3));
        tracker.mark_replay_frozen(20);
        sleep(Duration::from_millis(2));
        tracker.mark_bank_created(21, Some(20));
        let elapsed_us = begin.elapsed().as_micros() as f64;

        let after = split.map(read);
        let deltas: Vec<f64> = before
            .iter()
            .zip(&after)
            .map(|((count_before, sum_before), (count_after, sum_after))| {
                assert_eq!(count_after - count_before, 1);
                sum_after - sum_before
            })
            .collect();
        assert!(deltas[0] >= 3_000.0, "waited 3ms for the parent: {}", deltas[0]);
        assert!(deltas[1] >= 2_000.0, "2ms from parent freeze to bank: {}", deltas[1]);
        assert!(deltas[0] + deltas[1] <= elapsed_us);
    }

    #[test]
    fn test_bank_created_parent_frozen_first_has_zero_wait() {
        let _lock = PARENT_FROZEN_SPLIT_LOCK.lock().unwrap();
        let tracker = ExecutedTxLatencyTracker::new();
        tracker.mark_replay_frozen(30);
        tracker.mark_slot_started(31);
        let wait = crate::pipeline_metrics::SLOT_CONFIRMATION_DURATION_US
            .with_label_values(&["first_shred_to_parent_frozen"]);
        let sum_before = wait.get_sample_sum();
        tracker.mark_bank_created(31, Some(30));
        // Only check that this call added ~0 (the sum also holds earlier samples).
        assert!(wait.get_sample_sum() - sum_before < 1_000.0);
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
    fn test_executed_tx_tracker_last_shred_does_not_remove_start() {
        let tracker = ExecutedTxLatencyTracker::new();
        tracker.mark_slot_started(11);
        tracker.mark_last_shred(11);
        assert!(tracker.started.contains_key(&11));
        tracker.mark_tx_notified(11);
    }

    #[test]
    fn test_executed_tx_tracker_last_shred_with_no_start_does_not_panic() {
        let tracker = ExecutedTxLatencyTracker::new();
        tracker.mark_last_shred(999_995);
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
    fn test_tx_pipeline_tracker_no_data_set_start_is_a_noop() {
        let tracker = TxPipelineLatencyTracker::new();
        // No mark_data_set_started call -- mark_tx_data_set must not panic
        // and must not record a start.
        tracker.mark_tx_data_set([1u8; 64], 1, 0);
        assert!(!tracker.tx_started.contains_key(&[1u8; 64]));
        // mark_tx_notified with no recorded start must not panic either.
        tracker.mark_tx_notified([1u8; 64]);
    }

    #[test]
    fn test_tx_pipeline_tracker_full_flow() {
        let tracker = TxPipelineLatencyTracker::new();
        tracker.mark_data_set_started(5, 100);
        tracker.mark_tx_data_set([2u8; 64], 5, 100);
        assert!(tracker.tx_started.contains_key(&[2u8; 64]));
        tracker.mark_tx_notified([2u8; 64]);
        // Removed after notify.
        assert!(!tracker.tx_started.contains_key(&[2u8; 64]));
    }

    fn read_segments(names: &[&str]) -> Vec<(u64, f64)> {
        names
            .iter()
            .map(|name| {
                let h = TX_PIPELINE_SEGMENT_DURATION_US.with_label_values(&[name]);
                (h.get_sample_count(), h.get_sample_sum())
            })
            .collect()
    }

    #[test]
    fn test_tx_pipeline_segments_sum_to_total() {
        use std::{thread::sleep, time::Duration};
        let _lock = TX_PIPELINE_SEGMENT_LOCK.lock().unwrap();
        let segment_names = [
            "fetch_to_data_set_ready",
            "data_set_ready_to_replay_pickup",
            "replay_pickup_to_scheduled",
            "scheduled_to_exec_start",
            "exec_start_to_executed",
            "executed_to_committed",
            "commit_to_status_dequeued",
            "status_dequeued_to_notified",
        ];
        let before = read_segments(&segment_names);
        let whole_before = read_segments(&["replay_pickup_to_committed"]);

        let tracker = TxPipelineLatencyTracker::new();
        let begin = Instant::now();
        tracker.mark_data_set_started(9, 300);
        sleep(Duration::from_millis(2));
        tracker.mark_data_set_ready(9, 300);
        sleep(Duration::from_millis(2));
        tracker.mark_tx_data_set([7u8; 64], 9, 300);
        sleep(Duration::from_millis(2));
        tracker.mark_tx_scheduled([7u8; 64]);
        sleep(Duration::from_millis(2));
        tracker.mark_tx_exec_started([7u8; 64], false);
        sleep(Duration::from_millis(2));
        tracker.mark_tx_executed([7u8; 64], Instant::now());
        sleep(Duration::from_millis(2));
        let enqueued_at = Instant::now();
        sleep(Duration::from_millis(2));
        tracker.mark_tx_status_received([7u8; 64], enqueued_at);
        sleep(Duration::from_millis(2));
        tracker.mark_tx_notified([7u8; 64]);
        let elapsed_us = begin.elapsed().as_micros() as f64;

        let after = read_segments(&segment_names);
        let mut total = 0.0;
        for ((count_before, sum_before), (count_after, sum_after)) in before.iter().zip(&after) {
            assert_eq!(count_after - count_before, 1);
            let delta = sum_after - sum_before;
            assert!(delta >= 2_000.0, "each segment spans a 2ms sleep");
            total += delta;
        }
        // The segments telescope: together they cover the whole span (minus
        // per-segment microsecond truncation).
        assert!(
            (16_000.0..=elapsed_us).contains(&total),
            "{total} vs {elapsed_us}"
        );
        // The unsplit replay segment is not observed when the split is.
        assert_eq!(read_segments(&["replay_pickup_to_committed"]), whole_before);
    }

    #[test]
    fn test_tx_pipeline_missing_scheduler_stamps_fall_back_to_whole_segment() {
        let _lock = TX_PIPELINE_SEGMENT_LOCK.lock().unwrap();
        let split = [
            "replay_pickup_to_scheduled",
            "scheduled_to_exec_start",
            "exec_start_to_executed",
            "executed_to_committed",
        ];
        let split_before = read_segments(&split);
        let whole_before = read_segments(&["replay_pickup_to_committed"])[0].0;
        let other_before = read_segments(&["commit_to_status_dequeued"])[0].0;

        let tracker = TxPipelineLatencyTracker::new();
        tracker.mark_data_set_started(13, 0);
        tracker.mark_data_set_ready(13, 0);
        tracker.mark_tx_data_set([8u8; 64], 13, 0);
        // Scheduled and executed, but the exec-start stamp is missing.
        tracker.mark_tx_scheduled([8u8; 64]);
        tracker.mark_tx_executed([8u8; 64], Instant::now());
        tracker.mark_tx_status_received([8u8; 64], Instant::now());
        tracker.mark_tx_notified([8u8; 64]);

        assert_eq!(read_segments(&split), split_before);
        assert_eq!(
            read_segments(&["replay_pickup_to_committed"])[0].0 - whole_before,
            1
        );
        assert_eq!(
            read_segments(&["commit_to_status_dequeued"])[0].0 - other_before,
            1
        );
    }

    #[test]
    fn test_scheduler_queue_wait_labels() {
        use std::{thread::sleep, time::Duration};
        let _lock = TX_PIPELINE_SEGMENT_LOCK.lock().unwrap();
        let wait = &crate::pipeline_metrics::SCHEDULER_QUEUE_WAIT_DURATION_US;
        let read = |task: &str| {
            let h = wait.with_label_values(&[task]);
            (h.get_sample_count(), h.get_sample_sum())
        };
        let (blocked_count, blocked_sum) = read("blocked");
        let (unblocked_count, _) = read("unblocked");

        let tracker = TxPipelineLatencyTracker::new();
        tracker.mark_data_set_started(14, 0);
        tracker.mark_tx_data_set([9u8; 64], 14, 0);
        tracker.mark_tx_data_set([10u8; 64], 14, 0);
        tracker.mark_tx_scheduled([9u8; 64]);
        sleep(Duration::from_millis(2));
        tracker.mark_tx_exec_started([9u8; 64], true);
        // Not scheduled: no queue-wait sample.
        tracker.mark_tx_exec_started([10u8; 64], false);
        // Untracked signature: no-op.
        tracker.mark_tx_exec_started([11u8; 64], false);

        let (blocked_count_after, blocked_sum_after) = read("blocked");
        assert_eq!(blocked_count_after - blocked_count, 1);
        assert!(blocked_sum_after - blocked_sum >= 2_000.0);
        assert_eq!(read("unblocked").0, unblocked_count);
        let not_scheduled = tracker.tx_started.get(&[10u8; 64]).unwrap();
        assert!(not_scheduled.exec_start.is_some());
        drop(not_scheduled);
        assert!(!tracker.tx_started.contains_key(&[11u8; 64]));
    }

    #[test]
    fn test_data_set_completion_phases_sum_to_span() {
        use std::{thread::sleep, time::Duration};
        let durations = &crate::pipeline_metrics::DATA_SET_COMPLETION_DURATION_US;
        let completions = &crate::pipeline_metrics::DATA_SET_COMPLETIONS_TOTAL;
        let read = |phase: &str| {
            let h = durations.with_label_values(&[phase, "recovered"]);
            (h.get_sample_count(), h.get_sample_sum())
        };
        let phases = ["first_fetch_to_completing_recv", "completing_recv_to_ready"];
        let before = phases.map(read);
        let completions_before = completions.with_label_values(&["recovered"]).get();

        let tracker = TxPipelineLatencyTracker::new();
        let begin = Instant::now();
        tracker.mark_data_set_started(12, 64);
        sleep(Duration::from_millis(3));
        let completing_batch_received = Instant::now();
        sleep(Duration::from_millis(2));
        tracker.mark_data_set_ready(12, 64);
        tracker.mark_data_set_completed(12, 64, completing_batch_received, "recovered");
        let elapsed_us = begin.elapsed().as_micros() as f64;

        let after = phases.map(read);
        let deltas: Vec<f64> = before
            .iter()
            .zip(&after)
            .map(|((count_before, sum_before), (count_after, sum_after))| {
                assert_eq!(count_after - count_before, 1);
                sum_after - sum_before
            })
            .collect();
        assert!(deltas[0] >= 3_000.0, "3ms until the batch: {}", deltas[0]);
        assert!(deltas[1] >= 2_000.0, "2ms until ready: {}", deltas[1]);
        assert!(deltas[0] + deltas[1] <= elapsed_us);
        assert_eq!(
            completions.with_label_values(&["recovered"]).get() - completions_before,
            1
        );

        // No recorded start: only the completion is counted.
        let turbine_before = completions.with_label_values(&["turbine"]).get();
        let turbine_count_before = durations
            .with_label_values(&["completing_recv_to_ready", "turbine"])
            .get_sample_count();
        tracker.mark_data_set_completed(999_990, 0, Instant::now(), "turbine");
        assert_eq!(
            completions.with_label_values(&["turbine"]).get() - turbine_before,
            1
        );
        assert_eq!(
            durations
                .with_label_values(&["completing_recv_to_ready", "turbine"])
                .get_sample_count(),
            turbine_count_before
        );
    }

    #[test]
    fn test_tx_pipeline_tracker_multiple_tx_same_data_set() {
        let tracker = TxPipelineLatencyTracker::new();
        tracker.mark_data_set_started(6, 200);
        tracker.mark_tx_data_set([3u8; 64], 6, 200);
        tracker.mark_tx_data_set([4u8; 64], 6, 200);
        tracker.mark_tx_notified([3u8; 64]);
        tracker.mark_tx_notified([4u8; 64]);
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

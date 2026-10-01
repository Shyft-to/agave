//! Prometheus metric definitions for the shred-receive -> replay -> Geyser
//! notification pipeline. See `docs/perf/shred-to-geyser-prometheus-plan.md`
//! in the repo root for the design this implements.
//!
//! All durations are in **microseconds** (deliberate deviation from
//! Prometheus's usual base-unit-in-seconds convention).

use {
    crate::prometheus_metrics::{register_gauge_vec, register_histogram_vec, register_int_counter},
    prometheus::{GaugeVec, HistogramVec, IntCounter},
    std::sync::LazyLock,
};

/// Busy-duration of each shred-path operation, labeled by `stage`:
/// `receive`, `deserialize`, `dedup`, `filter`, `sign`, `retransmit`,
/// `verified_recv_wait` (time `window_service::run_insert` spends blocked in
/// `recv_timeout` waiting for the sigverify->window-service channel, plus
/// draining whatever had queued -- mostly reflects genuine idle wait unless
/// the channel has backlog, in which case pair it with
/// `VERIFIED_SHREDS_QUEUE_LENGTH`).
pub static SHRED_STAGE_DURATION_US: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "agave_shred_stage_duration_us",
        "Duration of shred pipeline stages, in microseconds",
        &["stage"],
    )
});

/// Depth of the fetch/verified channels feeding the shred pipeline, labeled
/// by `channel`.
pub static SHRED_FETCH_QUEUE_LENGTH: LazyLock<GaugeVec> = LazyLock::new(|| {
    register_gauge_vec(
        "agave_shred_fetch_queue_length",
        "Depth of shred pipeline channels",
        &["channel"],
    )
});

pub static SHRED_PACKETS_DROPPED_TOTAL: LazyLock<IntCounter> = LazyLock::new(|| {
    register_int_counter(
        "agave_shred_packets_dropped_total",
        "Shred packets dropped before reaching sigverify",
    )
});

/// Depth of the `verified_sender`/`verified_receiver` channel between
/// sigverify and `window_service::run_insert`, sampled once per `run_insert`
/// call right after `recv_timeout` succeeds (before the channel is drained
/// via `try_iter`, so it reflects genuine backlog rather than the emptiness
/// left behind by that same drain). Unbounded channel, added while chasing
/// an unaccounted gap between measured shred-path stages and the `deshred`
/// end-to-end p99 (see docs/perf/shred-to-geyser-prometheus-plan.md, "Phase 3
/// hypothesis 3").
pub static VERIFIED_SHREDS_QUEUE_LENGTH: LazyLock<prometheus::Gauge> = LazyLock::new(|| {
    crate::prometheus_metrics::register_gauge(
        "agave_verified_shreds_queue_length",
        "Depth of the verified-shreds channel between sigverify and window_service",
    )
});

/// Busy-duration of CompletedDataSetsService-specific work, labeled by
/// `stage`: `rocksdb_reread` (re-reading and re-deserializing shred entries
/// already held in memory by the window-service insert, per completed data
/// set) and `batch_total` (the whole drain-and-notify batch, reusing the
/// pre-existing `batch_measure` this service already computed for its legacy
/// datapoint).
pub static DESHRED_STAGE_DURATION_US: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "agave_deshred_stage_duration_us",
        "Duration of CompletedDataSetsService-specific stages, in microseconds",
        &["stage"],
    )
});

/// Depth of the `CompletedDataSetsService` channel, sampled each time its
/// receiver thread loops. Added alongside `DESHRED_STAGE_DURATION_US` while
/// investigating an unaccounted ~17-25ms gap between the sum of measured
/// shred-path stages and the `deshred` end-to-end p99 -- if this stays near 0
/// while `rocksdb_reread`/`batch_total` are small too, the gap is neither a
/// queueing backlog nor blockstore re-read cost.
pub static COMPLETED_DATA_SETS_QUEUE_LENGTH: LazyLock<prometheus::Gauge> = LazyLock::new(|| {
    crate::prometheus_metrics::register_gauge(
        "agave_completed_data_sets_queue_length",
        "Depth of the CompletedDataSetsService channel",
    )
});

/// Depth of the (currently unbounded) `TransactionStatusService` channel,
/// sampled each time its receiver thread loops. A growing value here means
/// notify_transaction is falling behind commit, which shows up as inflated
/// `agave_replay_stage_duration_us{stage="tx_status_queue_wait"}` and
/// `agave_end_to_end_duration_us{path="executed_tx"}` values without any of
/// the execute/commit stages themselves being slow.
pub static TX_STATUS_QUEUE_LENGTH: LazyLock<prometheus::Gauge> = LazyLock::new(|| {
    crate::prometheus_metrics::register_gauge(
        "agave_tx_status_queue_length",
        "Depth of the TransactionStatusService channel",
    )
});

/// Busy-duration of the blockstore shred-store operation, labeled by
/// `phase`: `total`, `insert_shreds`, `write_batch`, `recovery`,
/// `insert_lock`, `commit_working_sets`.
pub static BLOCKSTORE_STORE_DURATION_US: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "agave_blockstore_store_duration_us",
        "Duration of blockstore shred-store phases, in microseconds",
        &["phase"],
    )
});

/// Busy-duration of each replay-path operation, labeled by `stage`:
/// `read_blockstore`, `collect_entries`, `execute`, `commit`.
pub static REPLAY_STAGE_DURATION_US: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "agave_replay_stage_duration_us",
        "Duration of replay pipeline stages, in microseconds",
        &["stage"],
    )
});

/// Fine-grained breakdown of the `execute` stage above, labeled by `phase`:
/// `check`, `validate_fees`, `load`, `execute`, `store`, `program_cache`,
/// `filter_executable`, `collect_balances`, `collect_logs`,
/// `update_stakes_cache`, `update_executors`, `check_block_limits`. These
/// reuse Solana's own pre-existing `ExecuteTimings` cumulative counters
/// (snapshotted before/after each `execute` call and observed as a delta)
/// rather than adding new manual timers.
pub static EXECUTE_PHASE_DURATION_US: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "agave_execute_phase_duration_us",
        "Fine-grained breakdown of the execute stage, in microseconds, from ExecuteTimings",
        &["phase"],
    )
});

/// Deeper breakdown of program execution specifically (a subset of `execute`
/// above), labeled by `phase`: `serialize`, `create_vm`, `execute_inner`,
/// `deserialize`, `get_or_create_executor`,
/// `create_executor_register_syscalls`, `create_executor_load_elf`,
/// `create_executor_verify_code`, `create_executor_jit_compile`. Also reuses
/// Solana's own `ExecuteDetailsTimings` cumulative counters.
pub static EXECUTE_DETAIL_DURATION_US: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "agave_execute_detail_duration_us",
        "Deeper breakdown of program execution, in microseconds, from ExecuteDetailsTimings",
        &["phase"],
    )
});

/// Time spent inside a Geyser plugin notify callback, labeled by
/// `notifier`: `account_update`, `slot_status`, `deshred_transaction`,
/// `transaction`.
pub static GEYSER_NOTIFY_DURATION_US: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "agave_geyser_notify_duration_us",
        "Duration of Geyser plugin notify callbacks, in microseconds",
        &["notifier"],
    )
});

/// Count of `mark_notified` calls that found (or didn't find) a matching
/// `mark_started` entry, labeled by `outcome`: `tracked` or `untracked`. Added
/// as a direct sanity check on the `deshred` end-to-end tracker itself, after
/// ruling out every other candidate hop for an unaccounted latency gap --
/// `mark_started` is keyed by a shred's own `fec_set_index`, `mark_notified`
/// by a completed data set's *starting shred index*, which are assumed equal
/// but aren't guaranteed to be in every case (out-of-order arrival, repair,
/// partial fills). A high `untracked` rate means the `deshred` metric is
/// sampling an unrepresentative subset of data sets, not measuring what it
/// claims to.
pub static DESHRED_TRACKING_TOTAL: LazyLock<prometheus::IntCounterVec> = LazyLock::new(|| {
    crate::prometheus_metrics::register_int_counter_vec(
        "agave_deshred_tracking_total",
        "Whether mark_notified found a matching mark_started entry",
        &["outcome"],
    )
});

/// Slot lifecycle latency, labeled by `stage`:
/// - `first_shred_to_created_bank`: "replay wake-up latency" -- how long
///   between this validator first fetching a shred for the slot and replay
///   actually creating a bank for it. Came back small in practice (~19ms
///   p90), ruling this out as the explanation for `executed_tx`'s much
///   larger tail.
/// - `first_shred_to_frozen`: first shred fetched -> bank freeze (all
///   transactions in the slot executed and committed), measured directly.
///   Added because composing this from `created_bank_to_confirmed` minus
///   `frozen_to_confirmed` (two independently-computed percentiles) is not
///   valid math (p90(A) - p90(B) != p90(A - B)) -- this is the real number.
///   No transaction's `executed_tx` observation can exceed roughly this
///   value for its own slot (plus the already-measured small tx-status
///   queue/notify overhead), so comparing the two directly is the way to
///   tell whether `executed_tx`'s tail is explained by slow slots or
///   something else entirely.
/// - `created_bank_to_confirmed`: "time taken for votes to confirm a slot",
///   measured from when this validator first created a bank for the slot.
/// - `frozen_to_confirmed`: "time difference of bank freeze -- transaction
///   processing finished -- to slot marked confirmed". Confirmation is
///   driven by `OptimisticallyConfirmedBankTracker` aggregating cluster
///   votes, a subsystem that runs independently of transaction execution --
///   so this is not downstream replay latency, it's primarily
///   vote-propagation/aggregation time.
///
/// See `metrics/src/pipeline_latency.rs::SlotConfirmationLatencyTracker` and
/// `ExecutedTxLatencyTracker::mark_bank_created` for exactly where each
/// timestamp is taken.
pub static SLOT_CONFIRMATION_DURATION_US: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "agave_slot_confirmation_duration_us",
        "Slot confirmation latency (vote-driven), in microseconds",
        &["stage"],
    )
});

/// Top-line end-to-end latency, labeled by `path`: `deshred` (first shred
/// received for a data set -> deshred transaction notified) or
/// `executed_tx` (first shred received for a slot -> last transaction
/// notified for that slot).
pub static END_TO_END_DURATION_US: LazyLock<HistogramVec> = LazyLock::new(|| {
    register_histogram_vec(
        "agave_end_to_end_duration_us",
        "End-to-end shred-to-geyser latency, in microseconds",
        &["path"],
    )
});

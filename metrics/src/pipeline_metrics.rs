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
/// `receive`, `deserialize`, `dedup`, `filter`, `sign`, `retransmit`.
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

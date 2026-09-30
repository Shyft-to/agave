# Phase 2 baseline queries (Prometheus)

Run these in the Prometheus expression browser (or `curl -G --data-urlencode`)
against the node exposing `--metrics-listen-address`, over a time range where
it's been running under normal load for at least that long (adjust `[1h]` to
however long it's actually been up if less). Paste the results into the Progress
Log in `docs/perf/shred-to-geyser-prometheus-plan.md`.

Percentiles use `0.5`/`0.9`/`0.99` directly (swap in `0.95` etc. if wanted) since
this isn't run through Grafana's `$percentile` variable here.

## 1. End-to-end latency (top-line numbers)

```promql
histogram_quantile(0.5, sum(rate(agave_end_to_end_duration_us_bucket[1h])) by (le, path))
histogram_quantile(0.9, sum(rate(agave_end_to_end_duration_us_bucket[1h])) by (le, path))
histogram_quantile(0.99, sum(rate(agave_end_to_end_duration_us_bucket[1h])) by (le, path))
```

Sanity check (should be non-zero; near-zero for `path="deshred"` means no
deshred-transaction Geyser plugin is loaded, near-zero for `path="executed_tx"`
means no transaction-notifying plugin is loaded):

```promql
sum(rate(agave_end_to_end_duration_us_count[1h])) by (path)
```

## 2. Shred path stage duration

```promql
histogram_quantile(0.5, sum(rate(agave_shred_stage_duration_us_bucket[1h])) by (le, stage))
histogram_quantile(0.9, sum(rate(agave_shred_stage_duration_us_bucket[1h])) by (le, stage))
histogram_quantile(0.99, sum(rate(agave_shred_stage_duration_us_bucket[1h])) by (le, stage))
```

Throughput (batches/iterations per sec, not shreds/sec — see dashboard panel
description for why the unit differs per stage):

```promql
sum(rate(agave_shred_stage_duration_us_count[1h])) by (stage)
```

Health:

```promql
rate(agave_shred_packets_dropped_total[1h])
agave_shred_fetch_queue_length
```

(The queue-length gauge isn't wired up yet, so expect it empty — not a bug.)

## 3. Blockstore store

```promql
histogram_quantile(0.5, sum(rate(agave_blockstore_store_duration_us_bucket[1h])) by (le, phase))
histogram_quantile(0.9, sum(rate(agave_blockstore_store_duration_us_bucket[1h])) by (le, phase))
histogram_quantile(0.99, sum(rate(agave_blockstore_store_duration_us_bucket[1h])) by (le, phase))
sum(rate(agave_blockstore_store_duration_us_count[1h])) by (phase)
```

## 4. Replay & execution

```promql
histogram_quantile(0.5, sum(rate(agave_replay_stage_duration_us_bucket[1h])) by (le, stage))
histogram_quantile(0.9, sum(rate(agave_replay_stage_duration_us_bucket[1h])) by (le, stage))
histogram_quantile(0.99, sum(rate(agave_replay_stage_duration_us_bucket[1h])) by (le, stage))
sum(rate(agave_replay_stage_duration_us_count[1h])) by (stage)
```

Remember: `read_blockstore`/`collect_entries` are per-`confirm_slot()`-call,
`execute`/`commit` are per-transaction-batch — don't expect these four to line up
1:1 (see dashboard panel description).

## 5. Geyser notify cost

```promql
histogram_quantile(0.5, sum(rate(agave_geyser_notify_duration_us_bucket[1h])) by (le, notifier))
histogram_quantile(0.9, sum(rate(agave_geyser_notify_duration_us_bucket[1h])) by (le, notifier))
histogram_quantile(0.99, sum(rate(agave_geyser_notify_duration_us_bucket[1h])) by (le, notifier))
sum(rate(agave_geyser_notify_duration_us_count[1h])) by (notifier)
```

## 6. Transaction-status queue (added after the executed_tx tail investigation)

```promql
histogram_quantile(0.5, sum(rate(agave_replay_stage_duration_us_bucket{stage="tx_status_queue_wait"}[1h])) by (le))
histogram_quantile(0.9, sum(rate(agave_replay_stage_duration_us_bucket{stage="tx_status_queue_wait"}[1h])) by (le))
histogram_quantile(0.99, sum(rate(agave_replay_stage_duration_us_bucket{stage="tx_status_queue_wait"}[1h])) by (le))
agave_tx_status_queue_length
```

If `tx_status_queue_wait` is large and `agave_tx_status_queue_length` is
sustained above 0, that backlog (not execute/commit) explains the `executed_tx`
end-to-end tail.

## 7. Execute phase/detail breakdown (added while investigating the executed_tx tail)

```promql
histogram_quantile(0.5, sum(rate(agave_execute_phase_duration_us_bucket[1h])) by (le, phase))
histogram_quantile(0.9, sum(rate(agave_execute_phase_duration_us_bucket[1h])) by (le, phase))
histogram_quantile(0.99, sum(rate(agave_execute_phase_duration_us_bucket[1h])) by (le, phase))

histogram_quantile(0.5, sum(rate(agave_execute_detail_duration_us_bucket[1h])) by (le, phase))
histogram_quantile(0.9, sum(rate(agave_execute_detail_duration_us_bucket[1h])) by (le, phase))
histogram_quantile(0.99, sum(rate(agave_execute_detail_duration_us_bucket[1h])) by (le, phase))
```

Reuses Solana's own pre-existing `ExecuteTimings`/`ExecuteDetailsTimings` counters
(snapshotted before/after each `execute` call), not new manual timers. Watch
especially `create_executor_jit_compile` and the other `create_executor_*`
phases in the detail query -- those are ~0 for warm/cached programs and the
most likely place a rare outlier hides (a cold program forcing a JIT compile
on the hot path).

## 8. Completed-data-sets channel wait + RocksDB re-read (Phase 3 hypothesis 2)

```promql
histogram_quantile(0.5, sum(rate(agave_deshred_stage_duration_us_bucket[1h])) by (le, stage))
histogram_quantile(0.9, sum(rate(agave_deshred_stage_duration_us_bucket[1h])) by (le, stage))
histogram_quantile(0.99, sum(rate(agave_deshred_stage_duration_us_bucket[1h])) by (le, stage))
agave_completed_data_sets_queue_length
```

`stage="rocksdb_reread"` is the `get_entries_in_data_block` re-read/re-deserialize
cost per completed data set; `stage="batch_total"` is the whole
drain-and-notify batch. If `rocksdb_reread` and `batch_total` are both small
and the queue length stays near 0, this rules out this hop as the source of
the unaccounted ~17-25ms gap between measured shred-path stages and the
`deshred` end-to-end p99 (see the Progress Log entry for "Phase 3 hypothesis 2").

## 9. Host facts (not queries — note manually alongside the results above)

- `nproc`; `lscpu | head -20`
- The validator command line, especially `--tvu-receive-threads`,
  `--tvu-shred-sigverify-threads`
- Which Geyser plugin(s) are loaded and which event types each subscribes to
  (account update / slot status / deshred transaction / transaction)

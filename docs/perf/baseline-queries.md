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

## 9. Verified-shreds channel wait/backlog (Phase 3 hypothesis 3)

```promql
histogram_quantile(0.5, sum(rate(agave_shred_stage_duration_us_bucket{stage="verified_recv_wait"}[1h])) by (le))
histogram_quantile(0.9, sum(rate(agave_shred_stage_duration_us_bucket{stage="verified_recv_wait"}[1h])) by (le))
histogram_quantile(0.99, sum(rate(agave_shred_stage_duration_us_bucket{stage="verified_recv_wait"}[1h])) by (le))
agave_verified_shreds_queue_length
```

This is the last genuinely unmeasured hop between the shred-fetch tracker's
start point and the `deshred` end-to-end notify -- the unbounded channel
between sigverify and `window_service::run_insert`. If this also comes back
small/near-zero, the remaining ~45ms p99 gap (see the Progress Log's Phase 3
hypothesis 2 result) is more likely burst correlation across stages than a
single missing hop -- worth cross-referencing against
`agave_shred_stage_duration_us{stage="dedup"|"sign"}` and
`agave_blockstore_store_duration_us{phase="total"}` *max* values (not just
percentiles) in the same time window to check for simultaneous spikes.

## 10. Deshred tracker trustworthiness (Phase 3 hypothesis 4)

```promql
sum(rate(agave_deshred_tracking_total[1h])) by (outcome)
```

Compute `untracked / (tracked + untracked)`. Near-zero means the `deshred`
end-to-end metric is measuring what it claims to, and the unaccounted p99 gap
is most likely burst correlation across stages rather than any single fixable
hop. A high fraction means the metric itself is unreliable (sampling an
unrepresentative subset of data sets) and any conclusions drawn from it so
far -- including the Phase 3 hypothesis 1 (coalesce window) verdict -- should
be revisited.

## 11. Slot confirmation latency (votes, independent of transaction execution)

```promql
histogram_quantile(0.5, sum(rate(agave_slot_confirmation_duration_us_bucket[1h])) by (le, stage))
histogram_quantile(0.9, sum(rate(agave_slot_confirmation_duration_us_bucket[1h])) by (le, stage))
histogram_quantile(0.99, sum(rate(agave_slot_confirmation_duration_us_bucket[1h])) by (le, stage))
```

`stage="created_bank_to_confirmed"`: time taken for votes to confirm a slot,
from when this validator first created a bank for it. `stage=
"frozen_to_confirmed"`: time difference of bank freeze (transaction processing
finished) to slot marked confirmed -- this isolates pure vote-propagation/
aggregation time, since confirmation is driven by
`OptimisticallyConfirmedBankTracker` aggregating cluster votes, a subsystem
that runs independently of transaction execution/replay. If this is large
while `created_bank_to_confirmed` is only slightly larger, most of the gap is
voting, not this validator's own replay speed.

`stage="first_shred_to_created_bank"` ("replay wake-up latency"): added after
`executed_tx` (first-shred -> commit) turned out much larger than
`created_bank_to_frozen` (implied from the two queries above), even though
`notify_transaction` fires at commit with no dependency on voting at all --
the only place that gap could be hiding is between shred arrival and replay
actually creating a bank for the slot. Compare this directly against
`executed_tx` p90/p99 from section 1: if `first_shred_to_created_bank` alone
accounts for most of it, replay wake-up (not voting, not replay itself) is
the real bottleneck for `executed_tx`.

## 12. Host facts (not queries — note manually alongside the results above)

- `nproc`; `lscpu | head -20`
- The validator command line, especially `--tvu-receive-threads`,
  `--tvu-shred-sigverify-threads`
- Which Geyser plugin(s) are loaded and which event types each subscribes to
  (account update / slot status / deshred transaction / transaction)

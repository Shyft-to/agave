# Phase 2 baseline queries (InfluxQL)

Part of the shred → geyser latency plan (`docs/perf/shred-to-geyser-plan.md`). These replace the older, unfiltered queries in that file.

- **Every query is filtered to the test node**: `"host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'`. Many nodes report to the same database. Without the filter, the existing metrics (`shred_*`, `recv-window-insert-shreds`, `blockstore-insert-shreds`, queue peaks) are summed across all nodes. Only the new datapoints (`shred-geyser-latency`, `slot-geyser-latency`, `geyser-notify-*`) come from the test node alone.
- Every aggregate has an `AS` alias so the result table has readable column names.
- Grafana's table view has dropped the first selected column of each query, so every query starts with a throwaway `count(...) AS "ignore"` column. Ignore it in the results.
- Time range is the last hour. Run after at least 1 hour on the instrumented build.

## 0. Sanity check (must be non-zero)
If zero: the node isn't sending metrics to that database, isn't running the new build, or has no completed-data-sets service (needs RPC full API or a deshred plugin).
```sql
SELECT count("total_p50_us") AS "rows_last_10m"
FROM "shred-geyser-latency" WHERE time > now() - 10m AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 1a. Deshred end-to-end and per-stage delay
p50/p90/p99 are means of per-10s percentiles, so approximate.
```sql
SELECT count("data_sets") AS "ignore",
       mean("total_p50_us") AS "total_p50_us",
       mean("total_p90_us") AS "total_p90_us",
       mean("total_p99_us") AS "total_p99_us",
       max("total_max_us") AS "total_max_us",
       mean("fetch_to_sigverify_p90_us") AS "fetch_to_sigverify_p90_us",
       mean("sigverify_to_insert_p90_us") AS "sigverify_to_insert_p90_us",
       mean("insert_to_dequeue_p90_us") AS "insert_to_dequeue_p90_us",
       mean("dequeue_to_loaded_p90_us") AS "dequeue_to_loaded_p90_us",
       mean("loaded_to_notified_p90_us") AS "loaded_to_notified_p90_us",
       mean("arrival_spread_p90_us") AS "arrival_spread_p90_us",
       sum("data_sets") AS "data_sets_tracked",
       sum("untracked_data_sets") AS "untracked_data_sets"
FROM "shred-geyser-latency" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 1b. Tail per stage
```sql
SELECT count("data_sets") AS "ignore",
       max("fetch_to_sigverify_max_us") AS "fetch_to_sigverify_max_us",
       max("sigverify_to_insert_max_us") AS "sigverify_to_insert_max_us",
       max("insert_to_dequeue_max_us") AS "insert_to_dequeue_max_us",
       max("dequeue_to_loaded_max_us") AS "dequeue_to_loaded_max_us",
       max("loaded_to_notified_max_us") AS "loaded_to_notified_max_us",
       mean("fetch_to_sigverify_p99_us") AS "fetch_to_sigverify_p99_us",
       mean("sigverify_to_insert_p99_us") AS "sigverify_to_insert_p99_us",
       mean("dequeue_to_loaded_p99_us") AS "dequeue_to_loaded_p99_us",
       mean("loaded_to_notified_p99_us") AS "loaded_to_notified_p99_us"
FROM "shred-geyser-latency" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 1c. Every 10s window that had a slow data set
Timestamps to correlate with `blockstore-insert-shreds` and plugin activity.
```sql
SELECT "total_max_us", "fetch_to_sigverify_max_us", "sigverify_to_insert_max_us", "dequeue_to_loaded_max_us", "loaded_to_notified_max_us"
FROM "shred-geyser-latency" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp' AND "total_max_us" > 20000
```

## 2. Per-slot delays (executed txs and slot status)
The `>= 0` filter drops slots with a missing stage.
```sql
SELECT count("slot") AS "ignore",
       percentile("first_fetched_to_first_tx_us", 50) AS "first_tx_p50",
       percentile("first_fetched_to_first_tx_us", 90) AS "first_tx_p90",
       percentile("first_fetched_to_last_tx_us", 90) AS "last_tx_p90",
       percentile("completed_to_last_tx_us", 90) AS "completed_to_last_tx_p90",
       percentile("created_bank_to_replay_start_us", 90) AS "bank_to_replay_p90",
       percentile("replay_start_to_first_tx_us", 90) AS "replay_to_first_tx_p90",
       percentile("frozen_to_processed_us", 90) AS "frozen_to_processed_p90",
       percentile("processed_to_confirmed_us", 90) AS "proc_to_conf_p90",
       count("slot") AS "slots"
FROM "slot-geyser-latency" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp' AND "first_fetched_to_last_tx_us" >= 0
```

## 3. Plugin cost
Run once per measurement: `geyser-notify-account-update`, `geyser-notify-slot-status`, `geyser-notify-deshred-transaction`. Average = `total_us / calls`. An empty result means the plugin doesn't receive that kind of event.
```sql
SELECT count("count") AS "ignore",
       sum("total_us") AS "total_us",
       sum("count") AS "calls",
       max("max_us") AS "worst_call_us"
FROM "geyser-notify-account-update" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 4a. Receive
```sql
SELECT count("packets_count") AS "ignore",
       sum("packets_count") AS "packets_count",
       sum("packet_batches_count") AS "packet_batches_count",
       sum("full_packet_batches_count") AS "full_packet_batches_count",
       sum("num_packets_dropped") AS "num_packets_dropped",
       max("channel_len") AS "max_channel_len",
       max("fetch_max_batch_us") AS "fetch_max_batch_us",
       sum("fetch_elapsed_us") AS "fetch_elapsed_us",
       sum("fetch_idle_us") AS "fetch_idle_us"
FROM "shred_fetch_receiver" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```
```sql
SELECT count("shred_count") AS "ignore",
       sum("shred_count") AS "shred_count",
       sum("overflow_shreds") AS "overflow_shreds"
FROM "shred_fetch" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 4b. Sigverify
```sql
SELECT count("num_iters") AS "ignore",
       sum("num_iters") AS "num_iters",
       sum("num_batches") AS "num_batches",
       sum("num_packets") AS "num_packets",
       sum("num_duplicates") AS "num_duplicates",
       sum("num_discards_pre") AS "num_discards_pre",
       sum("num_discards_post") AS "num_discards_post",
       sum("num_retransmit_stage_overflow_shreds") AS "num_retransmit_stage_overflow_shreds",
       sum("recv_micros") AS "recv_micros",
       sum("recv_wait_micros") AS "recv_wait_micros",
       sum("sigverify_micros") AS "sigverify_micros",
       sum("resign_micros") AS "resign_micros",
       sum("elapsed_micros") AS "elapsed_micros"
FROM "shred_sigverify" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 4c. Window insert and blockstore
```sql
SELECT count("run_insert_count") AS "ignore",
       sum("run_insert_count") AS "run_insert_count",
       sum("num_shreds_received") AS "num_shreds_received",
       sum("shred_receiver_elapsed_us") AS "shred_receiver_elapsed_us",
       sum("shred_receiver_wait_us") AS "shred_receiver_wait_us",
       sum("shred_deserialize_elapsed_us") AS "shred_deserialize_elapsed_us",
       sum("blockstore_insert_elapsed_us") AS "blockstore_insert_elapsed_us"
FROM "recv-window-insert-shreds" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```
```sql
SELECT count("num_shreds") AS "ignore",
       sum("num_shreds") AS "num_shreds",
       sum("total_elapsed_us") AS "sum_total_elapsed_us",
       max("total_elapsed_us") AS "max_total_elapsed_us",
       max("insert_lock_elapsed_us") AS "max_insert_lock_elapsed_us",
       max("write_batch_elapsed_us") AS "max_write_batch_elapsed_us",
       max("shred_recovery_elapsed_us") AS "max_shred_recovery_elapsed_us",
       max("chaining_elapsed_us") AS "max_chaining_elapsed_us"
FROM "blockstore-insert-shreds" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 4d. Queue peaks
```sql
SELECT count("max_receiver_len") AS "ignore",
       max("max_receiver_len") AS "peak_queue_len"
FROM "entry-notifier-service-timing" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```
```sql
SELECT count("max_receiver_len") AS "ignore",
       max("max_receiver_len") AS "peak_queue_len"
FROM "transaction-status-service-timing" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 5. Host facts (from the node)
- `nproc`, and `lscpu | head -20`
- the validator command line, especially `--tvu-*` flags (`ps -o args= -C agave-validator`)
- which geyser plugin(s) are loaded, and whether they handle deshred transactions, account updates and transactions

If the first hour has too few samples, rerun with `AGAVE_PIPELINE_LATENCY_SLOT_SAMPLE=1`.

# Additional queries (added after baseline pull #3)

## 3b / 3c. Plugin cost for slot status and deshred transactions
Same query as section 3 with the measurement changed:
```sql
SELECT count("count") AS "ignore",
       sum("total_us") AS "total_us",
       sum("count") AS "calls",
       max("max_us") AS "worst_call_us"
FROM "geyser-notify-slot-status" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```
```sql
SELECT count("count") AS "ignore",
       sum("total_us") AS "total_us",
       sum("count") AS "calls",
       max("max_us") AS "worst_call_us"
FROM "geyser-notify-deshred-transaction" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 6a. Blockstore insert: per-phase time (where the ~188µs per run goes)
```sql
SELECT count("num_shreds") AS "ignore",
       sum("insert_lock_elapsed_us") AS "insert_lock_us",
       sum("insert_shreds_elapsed_us") AS "insert_shreds_us",
       sum("shred_recovery_elapsed_us") AS "shred_recovery_us",
       sum("chaining_elapsed_us") AS "chaining_us",
       sum("commit_working_sets_elapsed_us") AS "commit_working_sets_us",
       sum("write_batch_elapsed_us") AS "write_batch_us",
       sum("total_elapsed_us") AS "total_us"
FROM "blockstore-insert-shreds" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 6b. Slow blockstore inserts, with timestamps (correlate with the 1c windows)
```sql
SELECT "total_elapsed_us", "write_batch_elapsed_us", "shred_recovery_elapsed_us", "num_shreds"
FROM "blockstore-insert-shreds" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp' AND "total_elapsed_us" > 100000
```

## 6c. Transaction status service (executed-tx path)
```sql
SELECT count("batch_count") AS "ignore",
       sum("batch_count") AS "batches",
       sum("transaction_count") AS "transactions",
       sum("notify_transaction_count") AS "notified",
       sum("notify_transaction_elapsed_us") AS "notify_us",
       sum("write_batch_elapsed_us") AS "write_batch_us"
FROM "transaction-status-service-timing" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 6d. Fetch modifier time (`solTvuPktMod`)
```sql
SELECT count("packets_count") AS "ignore",
       sum("modifier_elapsed_us") AS "modifier_elapsed_us"
FROM "shred_fetch_receiver" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

# Queries for the build with the receive histogram and sigverify breakdown

Needs the build that adds `fetch_batch_p*_us` and the sigverify breakdown fields (commit "shred pipeline: batching histogram, sigverify breakdown and A/B knobs"). Before/after runs should record the environment variables in effect.

## 7a. Batching delay at the receiver (how long the oldest packet of a batch waits)
Percentiles are upper bounds of 250µs buckets; 10000 means "10ms or more". Averaged over 1s reports.
```sql
SELECT count("packets_count") AS "ignore",
       mean("fetch_batch_p50_us") AS "fetch_batch_p50_us",
       mean("fetch_batch_p90_us") AS "fetch_batch_p90_us",
       mean("fetch_batch_p99_us") AS "fetch_batch_p99_us",
       max("fetch_max_batch_us") AS "fetch_max_batch_us",
       sum("packet_batches_count") AS "packet_batches_count",
       sum("full_packet_batches_count") AS "full_packet_batches_count",
       sum("packets_count") AS "packets_count"
FROM "shred_fetch_receiver" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## 7b. Sigverify per-iteration breakdown
Compare `elapsed_micros` with the sum of the parts; per iteration = value / `num_iters`.
```sql
SELECT count("num_iters") AS "ignore",
       sum("num_iters") AS "num_iters",
       sum("num_serial_iters") AS "num_serial_iters",
       sum("num_packets") AS "num_packets",
       sum("elapsed_micros") AS "elapsed_micros",
       sum("dedup_micros") AS "dedup_micros",
       sum("bank_forks_micros") AS "bank_forks_micros",
       sum("sigverify_micros") AS "sigverify_micros",
       sum("resign_micros") AS "resign_micros",
       sum("extract_micros") AS "extract_micros",
       sum("send_micros") AS "send_micros"
FROM "shred_sigverify" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'
```

## A/B knobs (environment variables of the validator process, read once at startup)
| Variable | Default | Meaning |
|---|---|---|
| `AGAVE_SHRED_FETCH_COALESCE_US` | `5000` | Time a shred receiver keeps waiting for more packets after the first before forwarding a partly filled batch. `0` = forward as soon as the socket is drained. Logged at startup as "shred fetch coalesce window". Applies to the turbine and repair receivers. |
| `AGAVE_SHRED_SIGVERIFY_SERIAL_MAX_PACKETS` | `0` (off) | Sigverify iterations with at most this many packets run dedup, verification and resign on the sigverify thread instead of the rayon pool. Try `256`. `num_serial_iters` shows how many iterations used it. |
| `AGAVE_PIPELINE_LATENCY_SLOT_SAMPLE` | `2` | Track every N-th slot in `shred-geyser-latency` / `slot-geyser-latency`; `0` disables. |

Suggested runs, one change at a time, at least 30 minutes each at a comparable time of day, comparing 1a/1b, 7a and 7b:
1. Baseline of this build (all defaults).
2. `AGAVE_SHRED_SIGVERIFY_SERIAL_MAX_PACKETS=256`.
3. `AGAVE_SHRED_FETCH_COALESCE_US=1000`, then `500`, then `0`, ideally together with the serial setting from run 2 if it helped.

Remember `shred-geyser-latency` starts at the fetch modifier, so it cannot show the batching change directly; use the 7a percentiles for that (expected added delay per packet is about half of the batch call time).

## 6b (corrected). Slow blockstore inserts
The 100ms threshold in 6b was too low: the mean total per 2s report is ~133ms, so about half of all reports matched. The worst report in the hour was 252ms. Use:
```sql
SELECT "total_elapsed_us", "insert_shreds_elapsed_us", "write_batch_elapsed_us", "shred_recovery_elapsed_us", "num_shreds"
FROM "blockstore-insert-shreds" WHERE time > now() - 1h AND "host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp' AND "total_elapsed_us" > 200000
```

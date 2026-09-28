# Shred → Geyser Latency: Performance Investigation & Improvement Plan

> **How to resume:** read this whole doc, find the latest row in the Progress Log at the bottom,
> and continue with the next phase or item. After each step, append a row (date / step / result / commit).

## Context
We run a custom Agave build (branch `custom-pipeline`, forked from `2e10d67f90` "Bump version to 4.3.0-rc.1") whose purpose is to deliver data to Geyser plugins as fast as possible after shreds arrive. Some timing metrics already exist (commits `2f4c15fea4`, `96bb647e98`). They sum per-stage durations, and several include idle/blocking wait. Nothing measures **end-to-end age** (shred received → Geyser notified), so we cannot tell where latency actually goes. An earlier core-pinning attempt was reverted (`30850fbd71`/`cca33685d0` → reverted).

**Targets (user-selected), in priority order:**
1. **Deshred transactions**: shred → sigverify → blockstore insert → CompletedDataSetsService → `notify_deshred_transaction` (earliest, pre-replay)
2. **Executed transactions + account updates**: replay → execution → TransactionStatusService / accounts-db notify
3. **Slot status**: FirstShredReceived / Completed / CreatedBank / Processed / Confirmed / Rooted

**Measurement setup:** a live test node (mainnet/testnet RPC) running this branch with an InfluxDB metrics sink. Every change gets a before/after comparison there.

**Persistence:** this plan is committed as `docs/perf/shred-to-geyser-plan.md` in the repo, with a Progress Log that later conversations update. A memory entry points to it.

---

## 1. Architecture map (as of fork point + custom commits)

Wiring: `core/src/tvu.rs:380-501`, `core/src/validator.rs`.

```
UDP tvu sockets ×N ─► solRcvrShredNN ─► EvictingSender bounded(65536) ─► solTvuPktMod ─┐
UDP repair socket  ─► solRcvrShredRep00 ─► EvictingSender(65536) ─► solTvuRepPktMod ────┤ fetch_sender bounded(65536)
                                                                                        ▼
                                             solShredVerifr (+rayon solSvrfyShredNN)
                       retransmit_sender EvictingSender(16384) │   verified_sender UNBOUNDED
                                ▼                               ▼
               solRetransmittr (+solRetransmitNN)      solWinInsert (+pool ≤8 solWinInsertNN)
                 └─ FirstShredReceived → geyser          ├─ Blockstore::insert_shreds_at_location_handle_duplicate
                                                         ├─ recovered shreds → retransmit_sender
                                                         ├─ send_signals → ReplayStage / completed-slots
                                                         └─ completed_data_sets bounded(100_000)
                                                                   ▼
                                                   solComplDataSet: re-read entries from RocksDB,
                                                   ALT load on root bank, notify_deshred_transaction (inline)
ReplayStage (solReplayStage, solReplayFork) → confirm_slot → EntryNotification (unbounded → solEntryNotif)
   → unified scheduler solScHandle* → execute_batch (1 tx/batch)
        ├─ commit → accounts notify INLINE (geyser_plugin_utils.rs) 
        └─ tx clone → unbounded → solTxStatusWrtr → notify_transaction (2nd clone)
   → freeze → BankNotification → solOpConfBnkTrk → solBankNotif → slot status Processed/Confirmed/Rooted
   → block metadata notify INLINE on replay thread
```

### Stage table (files, threads, existing metrics)

| # | Stage | Key code | Threads | Existing datapoint / fields |
|---|---|---|---|---|
| S1 | UDP recv | `streamer/src/streamer.rs` `recv_loop` :173, `recvmmsg.rs` :94; `coalesce=Some(5ms)`, 64 pkts/batch, 1s poll | `solRcvrShredNN` (`--tvu-receive-threads`, default 1) | `shred_fetch_receiver`: packets_count, channel_len, num_packets_dropped, custom `fetch_elapsed_us` (includes idle wait) |
| S2 | Fetch filter | `core/src/shred_fetch_stage.rs` `modify_packets` :49, `ledger/src/shred/filter.rs` `should_discard_packet` :243 | `solTvuPktMod`, `solTvuRepPktMod` | `shred_fetch`: overflow_shreds, …; custom `modifier_elapsed_us` (excludes send) |
| S3 | Sigverify + dedup + resign | `turbine/src/sigverify_shreds.rs` `run_shred_sigverify` :142 (batch ≤1024 PacketBatches), `ledger/src/sigverify_shreds.rs` | `solShredVerifr` + `solSvrfyShredNN` | `shred_sigverify`: elapsed_micros, resign_micros, custom `recv_micros` (includes wait), `sigverify_micros` |
| S4 | Retransmit (parallel to S5) | `turbine/src/retransmit_stage.rs` `retransmit` :321 | `solRetransmittr` + 12 | `retransmit-stage`, `retransmit-stage-slot-stats` (outset_timestamp) |
| S5 | Window insert | `core/src/window_service.rs` `run_insert` :225 (drains the channel with no cap); `ledger/src/blockstore.rs` `do_insert_shreds` :2213, recovery :1809 | `solWinInsert` + ≤8 | `recv-window-insert-shreds`, `blockstore-insert-shreds` (insert_lock, recovery, write_batch …) |
| S6 | Completed data sets → deshred geyser | `core/src/completed_data_sets_service.rs` :141, `get_entries_in_data_block` :168, `load_transaction_addresses` :68, `notify_…` :236; `geyser-plugin-manager/src/deshred_transaction_notifier.rs` :32 | `solComplDataSet` (1 thread) | `deshred_geyser_timing`: batch_total_us, notify_total_us, lut_load_total_us, … |
| S7 | Replay load + entry notify | `core/src/replay_stage.rs` `replay_active_banks` :3775; `ledger/src/blockstore_processor.rs` `confirm_slot` :1372 | `solReplayStage`, `solReplayFork*`, `solEntryNotif` | `replay-slot-stats` (+ custom `notifier_schedule_elapsed_us`), `entry-notifier-service-timing` |
| S8 | Execution + account notify | `runtime/src/transaction_execution.rs` `execute_batch` :57; `runtime/src/bank.rs` commit :4356; `accounts-db/src/accounts_db/geyser_plugin_utils.rs` :10 | `solScHandle*` | only `store_accounts_us` |
| S9 | Tx status → geyser | `rpc/src/transaction_status_service.rs` :109; `geyser-plugin-manager/src/transaction_notifier.rs` :26 | `solTxStatusWrtr` | custom `transaction-status-service-timing` |
| S10 | Slot status | `slot_status_notifier.rs`; callers: retransmit :907 (FirstShred), `rpc_completed_slots_service.rs:51` (Completed), replay :5480 (CreatedBank), `slot_status_observer.rs:47` (Processed/Confirmed/Rooted) | various | none |
| S11 | Block metadata | `replay_stage.rs` :4262-4297 (inline, while holding replay locks) | `solReplayStage` | custom `geyser-notify-block-metadata` |

---

## 2. Phase 0: Persist the plan (first action after approval)
1. Write this plan to `docs/perf/shred-to-geyser-plan.md` and add a **Progress Log** section at the end: a table with columns date / step / result / commit.
2. Add a memory file `shred-geyser-perf-plan.md` (type `project`) pointing to that doc and summarizing the current phase. Add its line to `MEMORY.md`.
3. Commit the doc on `custom-pipeline`.
4. **Convention for future sessions:** read the doc first, continue from the first unchecked item, and append results to the Progress Log after each step.

## 3. Phase 1: Correct instrumentation (measure before optimizing)

### 1a. Fix misleading existing metrics
Split each of these into a busy part and a wait part:
- `fetch_elapsed_us`
- `recv_micros` (sigverify)
- `shred_receiver_elapsed_us` (window)

Method: time the blocking `recv`/`recv_timeout` separately from the `try_iter` drain and processing. Also:
- Include the `try_send` in `modifier_elapsed_us`.
- Restore the `handle_packets_elapsed_us` field name as an alias next to the new name, so upstream dashboards keep working.

### 1b. End-to-end, sampled latency tracker (the key missing piece)
Add a small module, e.g. `ledger/src/pipeline_latency.rs` (the ledger crate is visible to streamer consumers, turbine, core and rpc).
- **Key:** `(slot, fec_set_index)`.
- **Storage:** a fixed-size, lock-light map, e.g. `Mutex<LruCache>` touched once per FEC set rather than per shred, or a `DashMap`.
- **Sampling:** track only the first shred seen per FEC set, so the overhead is negligible.
- **Timestamps recorded:**
  - `t_recv`: batch receive `Instant`, taken in `recv_loop` right after `recv_from`. Carry it through as a timestamp stored per `PacketBatch`, or record it in the fetch modifier, which is the first place the slot/fec are parsed via `shred::layout`.
  - `t_fetch_out` (after the S2 send)
  - `t_sigverify_out` (just before `verified_sender.send`)
  - `t_insert_done` (after the blockstore write in `run_insert`)
  - `t_dataset_complete` (when `CompletedDataSetInfo` is emitted)
  - `t_deshred_notified` (after `notify_deshred_transaction` for that data set)
- **Per-slot timestamps**, in the same tracker keyed by slot:
  - first shred recv
  - slot full
  - bank created
  - replay start
  - first tx executed
  - bank frozen
  - last tx notified in TSS
  - Processed / Confirmed / Rooted notified
- **Emission:** on LRU eviction, emit `datapoint_info!("shred-geyser-latency", …)` with the stage deltas in µs plus p50/p90/p99 per 2s window. Use `solana_metrics::histogram` or a simple sorted sample vec.
- **Reuse:** `retransmit-stage-slot-stats` (`retransmit_stage.rs` :814/:871) already tracks per-slot outset timestamps. Use it for cross-checking.

### 1c. Per-plugin notify cost
Report the `Measure` that `deshred_transaction_notifier.rs` computes but never emits. Add the same kind of timing around account-update and slot-status notifies (counter + µs), so we can separate plugin cost from pipeline cost.

**Deliverable:** a Grafana/Influx query set, stored in the doc, that shows the stacked stage breakdown for deshred-tx latency and per-slot replay→tx-notify latency.

## 4. Phase 2: Baseline
- Run the instrumented build on the test node for at least 1 hour at normal mainnet load.
- Record p50/p90/p99 per stage delta in the Progress Log.
- Record host facts:
  - CPU model and cores
  - NIC and IRQ affinity
  - `--tvu-receive-threads`, `--tvu-shred-sigverify-threads`
  - geyser plugins loaded and their config
- Rank the stages by their contribution to p90 end-to-end latency. **Phase 3 work is taken strictly in that order.**

## 5. Phase 3: Improvement candidates (hypotheses; each gated by baseline data)

For each candidate: one commit or branch, a before/after comparison on the same node, and the result logged. Keep a change only if p90 improves and CPU and drop counters don't regress.

### Deshred path (priority 1)
1. **S1 coalesce window.** `recv_loop` for turbine uses `coalesce = Some(5ms)`, so a partly filled batch can sit up to 5ms before it is forwarded.
   - Try 0 / 500µs / 1ms (make it a CLI flag).
   - Also try `--tvu-receive-threads` > 1, since there are multiple TVU sockets.
   - Likely the largest cheap win.
2. **S3 batch size.** Sigverify drains up to 1024 PacketBatches per iteration. A large batch delays the first shred until the whole batch is verified.
   - Test smaller caps.
   - Check resign cost (`resign_micros`).
   - Check whether `shreds.clone()` for retransmit copies the payload. If `Payload` is not refcounted, make it so or restructure.
3. **S5 window insert batch size.** `run_insert` drains the channel with no cap (`try_iter().flatten()`). Under bursts this makes big RocksDB write batches and delays `CompletedDataSetInfo`.
   - Test a cap, e.g. 1–4k shreds.
   - Look at `insert_lock_elapsed_us`, `write_batch_elapsed_us` and `shred_recovery_elapsed_us`.
   - Reed-Solomon recovery is single-threaded per call; consider parallelizing across FEC sets.
4. **S6: remove the RocksDB re-read.**
   - Problem: `CompletedDataSetsService` calls `blockstore.get_entries_in_data_block`, which reads the shreds back from RocksDB and deserializes them again, even though `run_insert` already holds those shreds in memory.
   - Fix: have `insert_shreds` return, for each completed data set, the data-shred payloads it already holds (or the deserialized `Vec<Entry>`), and send those over the channel.
   - Expected: removes DB reads from the hottest geyser path.
5. **S6: parallelize and cache.**
   - Deserialize entries and notify in parallel across data sets. Ordering per slot must be preserved: shard by slot, or give each data set's work to a small pool.
   - Cache ALT lookups (`load_transaction_addresses`) per (table, root slot).
   - Avoid the `bank_forks.read()` per batch where possible.
6. **Channel hygiene.** `verified_sender` is unbounded. Check `max_receiver_len` trends. If queues build, the fix is throughput, not bounding.

### Executed tx / accounts path (priority 2)
7. **Replay wake-up latency.**
   - How quickly does ReplayStage react to the new-shred signal from `send_signals`? Check the replay loop timeout and wait logic in `replay_stage.rs`.
   - Measure slot-full → replay-start, and first-shred → first-tx-executed. Replay starts on partial slots, so the gap should be small.
8. **TSS clones.**
   - `transaction_execution.rs` :116-121 does `into_owned()`, and TSS then does a second `to_versioned_transaction()`.
   - Pass `Arc`/owned data once and skip the second clone.
   - One channel message per tx: consider batching per scheduler handler.
9. **Account notify inline on execution threads.** This is plugin-bound. Measure it (1c) first. If it is large, the options are a plugin-side async queue (preferred, no validator change) or a validator-side handoff channel.
10. **Block metadata on the replay thread.** Reward string formatting happens while replay locks are held. Move it off-thread (send to a notifier thread) or hoist it out of the locked section.

### Slot status (priority 3)
11. **FirstShredReceived** runs synchronously on `solRetransmittr`. A slow plugin delays retransmit. Consider a dedicated notifier thread.
12. **Processed/Confirmed/Rooted** hop through two threads and unbounded channels (`solOpConfBnkTrk` → `solBankNotif`). Measure the hop latency and collapse it if it is significant.

### System level (after code-level work; revisit the reverted pinning with data)
13. Core pinning or isolation for `solRcvrShred*`, `solShredVerifr`, `solWinInsert`, `solComplDataSet`, `solReplayStage`. Before trying again, record in the doc why the previous pinning attempt was reverted.
14. NIC RSS/IRQ affinity aligned with receiver threads. Larger `SO_RCVBUF`; check kernel UDP drops (`netstat -su`).
15. RocksDB tuning for the shred columns: WAL, memtable, compaction threads.

## 6. Workflow per change
- Branch or commit on `custom-pipeline`, one hypothesis per commit, with the metric name(s) it should move stated in the commit message.
- Before deploying: `cargo check -p <crate>` and the relevant unit tests.
  - `cargo test -p solana-core window_service`
  - `cargo test -p solana-turbine sigverify`
  - `cargo test -p solana-ledger blockstore::tests::test_insert`
  - `cargo test -p solana-core completed_data_sets`
- Deploy on the test node, then collect at least 30 min per config at a comparable time of day.
- Append a Progress Log row: before and after p50/p90/p99 for the targeted delta and end-to-end, CPU, drops, and a keep/revert decision.

## 7. Verification
- **Instrumentation sanity:** the sum of stage deltas ≈ end-to-end delta. Busy+wait splits add up to the old totals. The overhead of the tracker (CPU for the solWinInsert and sigverify threads) is under 1%.
- **Correctness:** no increase in the `shred_sigverify` discard, `blockstore-insert-shreds` error or `overflow_shreds` counters. Replay keeps up (the root distance to the cluster is unchanged). The geyser plugin receives the same transaction count per slot (compare `deshred_geyser_timing.transactions_count` against the executed count).
- **Outcome:** a documented p90 reduction in shred-recv → deshred-notify, and in first-shred → tx-notified per slot, with each change attributed in the Progress Log.

## Phase 1 status (updated 2026-09-28)
- [x] 1a: busy/wait split of `fetch_elapsed_us`, `recv_micros`, `shred_receiver_elapsed_us`; `modifier_elapsed_us` now includes the send; `handle_packets_elapsed_us` re-emitted as an alias.
- [x] 1b (deshred path): `ledger/src/pipeline_latency.rs`, datapoint `shred-geyser-latency`.
- [x] 1b (per-slot): `slot-geyser-latency`, one datapoint per sampled slot when it is rooted. Stages: first shred fetched → FirstShredReceived/Completed/CreatedBank notified → replay start → first/last tx notified by `solTxStatusWrtr` → frozen → Processed/Confirmed/Rooted notified.
- [x] 1c: `geyser-notify-account-update`, `geyser-notify-slot-status`, `geyser-notify-deshred-transaction` (count/total_us/avg_us/max_us every 2s).
- [ ] Deploy to the test node (done by the user) and run the Phase 2 baseline.

### Notes and deviations from the plan text above
- **Coalescing:** in `streamer/src/packet.rs` `recv_from_coalesce`, the deadline is computed at call start, not at first packet arrival. If the socket was idle for longer than `max_wait`, the batch is forwarded immediately. Under load the call starts right after the previous batch, so a batch can wait up to the full 5ms. New metrics: `fetch_max_batch_us` (upper bound on batching delay), `fetch_idle_us`.
- **Tracker start point:** `t_fetch` is taken when the modifier dequeues the batch, so socket batching and the `solRcvrShred*` → `solTvuPktMod` queue wait are not in `shred-geyser-latency`. Read `fetch_max_batch_us` and `channel_len` alongside it.
- **Tracker method:** every data shred of every N-th slot is tracked (`AGAVE_PIPELINE_LATENCY_SLOT_SAMPLE`, default 2, 0 disables). Per completed data set the timeline of the *last arriving* shred is reported, so network arrival spread is excluded (reported separately as `arrival_spread_*`). Data sets with no fetch/sigverify timestamps (recovered or repaired shreds) count as `untracked_data_sets`.
- **Legacy shreds:** this tree only parses Merkle shred variants (`ShredVariant::try_from` rejects legacy), which matters for hand-built test shreds.
- **Crate feature:** `solana-geyser-plugin-manager` tests need `--features agave-unstable-api`, otherwise they silently run 0 tests.

### Influx queries (InfluxQL)


```sql
 WHERE ("host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp') AND
```
Deshred end-to-end latency, percentiles per stage (stack the p50/p90 series):
```sql
SELECT mean("total_p50_us"), mean("total_p90_us"), mean("total_p99_us"), max("total_max_us")
FROM "shred-geyser-latency" WHERE $timeFilter GROUP BY time($__interval) fill(null)

SELECT mean("fetch_to_sigverify_p90_us"), mean("sigverify_to_insert_p90_us"), mean("insert_to_dequeue_p90_us"),
       mean("dequeue_to_loaded_p90_us"), mean("loaded_to_notified_p90_us")
FROM "shred-geyser-latency" WHERE $timeFilter GROUP BY time($__interval) fill(null)
```
Sample health (should be mostly tracked): `SELECT sum("data_sets"), sum("untracked_data_sets") FROM "shred-geyser-latency" WHERE $timeFilter GROUP BY time($__interval)`

Plugin cost (average and worst call per 2s window):
```sql
SELECT sum("total_us")/sum("count") AS avg_us, max("max_us") FROM "geyser-notify-account-update" WHERE $timeFilter GROUP BY time($__interval)
SELECT sum("total_us")/sum("count") AS avg_us, max("max_us") FROM "geyser-notify-slot-status" WHERE $timeFilter GROUP BY time($__interval)
SELECT sum("total_us")/sum("count") AS avg_us, max("max_us") FROM "geyser-notify-deshred-transaction" WHERE $timeFilter GROUP BY time($__interval)
```
Per-slot latency (one row per sampled slot; `-1` means a stage was not seen, later-before-earlier is clamped to 0):
```sql
SELECT "slot", "tx_count", "first_fetched_to_first_tx_us", "first_fetched_to_last_tx_us", "completed_to_last_tx_us",
       "created_bank_to_replay_start_us", "replay_start_to_first_tx_us", "frozen_to_last_tx_us"
FROM "slot-geyser-latency" WHERE $timeFilter AND "first_fetched_to_last_tx_us" >= 0

SELECT mean("frozen_to_processed_us"), mean("processed_to_confirmed_us"), mean("confirmed_to_rooted_us"), mean("first_fetched_to_confirmed_us")
FROM "slot-geyser-latency" WHERE $timeFilter GROUP BY time($__interval)
```
Percentiles over slots: `SELECT percentile("first_fetched_to_first_tx_us", 90) FROM "slot-geyser-latency" WHERE $timeFilter AND "first_fetched_to_first_tx_us" >= 0 GROUP BY time($__interval)`.

How to read `slot-geyser-latency` (things that can mislead):
- `first_fetched_*` starts at the first turbine data shred of the slot passing the fetch modifier. Leader slots and slots first seen by repair have no such start, so their `first_fetched_*` fields are `-1`.
- Slot-status stages are stamped when the plugins have returned from the callback, not when the event was produced. A slow plugin therefore delays the stamp and also delays the events queued behind it on the same thread (`solBankNotif`, `solRetransmittr`, `solRpcComplSlot`).
- `first_tx`/`last_tx` are stamped after `notify_transaction` returns on `solTxStatusWrtr`. Without a transaction-notifier plugin, `tx_count` is 0 and these are `-1`.
- Only every N-th slot is tracked (`AGAVE_PIPELINE_LATENCY_SLOT_SAMPLE`); slots that never root (dead or pruned forks) are dropped after 64 slots without a datapoint.
- A slot that is rooted before this build sees its early stages (e.g. right after restart) will have `-1` fields.

Stage busy vs idle (per second): `shred_fetch_receiver`: `fetch_elapsed_us`, `fetch_idle_us`, `fetch_max_batch_us`, `modifier_elapsed_us`, `channel_len`; `shred_sigverify`: `recv_micros`, `recv_wait_micros`, `sigverify_micros`, `resign_micros`; `recv-window-insert-shreds`: `shred_receiver_elapsed_us`, `shred_receiver_wait_us`, `shred_deserialize_elapsed_us`, `blockstore_insert_elapsed_us`.

### Phase 2 baseline: what to collect and paste
> **SUPERSEDED: use `docs/perf/baseline-queries.md`.** The queries below have no host filter, so the existing metrics (`shred_*`, `recv-window-insert-shreds`, `blockstore-insert-shreds`, queue peaks) are summed across all nodes that report to the same database, and they have no `AS` aliases. The new file filters on the test node (`"host_id"::tag = 'DXxxrCCvvGjayejfg56Yb2FgeZe2WVioCm4xzEjiyvhp'`) and aliases every aggregate.
>
> **Baseline pulls #1 and #2 in "Phase 2 baseline results" were made with the unfiltered queries.** Only `shred-geyser-latency`, `slot-geyser-latency` and `geyser-notify-*` are test-node-only, so those numbers stand. The conclusions that rely on the multi-node aggregates (resign = 97% of sigverify busy time, 62% duplicate packets, 67µs per batch, window-insert idle time) are unverified until the filtered queries are rerun.

Run after at least 1 hour on the instrumented build, with the time range set to the last hour. Use Grafana Explore or the `influx` CLI against the database the validator reports to (`SOLANA_METRICS_CONFIG` must be set on the node). Paste the results into the Progress Log, or into the next session. Groups 1 and 2 are enough to rank the stages; 3 and 4 explain the ranking.

Sanity check first: `SELECT count("total_p50_us") FROM "shred-geyser-latency" WHERE time > now() - 10m` must be non-zero. If it is zero, the node isn't sending metrics to that database, isn't running the new build, or has no completed-data-sets service (needs RPC full API or a deshred plugin).

**1. Deshred end-to-end and per-stage delay** (p50/p90/p99 fields are means of per-10s percentiles, so approximate; enough for ranking)
```sql
SELECT mean("total_p50_us"), mean("total_p90_us"), mean("total_p99_us"), max("total_max_us"),
       mean("fetch_to_sigverify_p90_us"), mean("sigverify_to_insert_p90_us"),
       mean("insert_to_dequeue_p90_us"), mean("dequeue_to_loaded_p90_us"),
       mean("loaded_to_notified_p90_us"), mean("arrival_spread_p90_us"),
       sum("data_sets"), sum("untracked_data_sets")
FROM "shred-geyser-latency" WHERE time > now() - 1h
```

**2. Per-slot delays (executed txs and slot status)**. The `>= 0` filter drops slots with a missing stage; report `count("slot")` too.
```sql
SELECT percentile("first_fetched_to_first_tx_us", 50) AS first_tx_p50, percentile("first_fetched_to_first_tx_us", 90) AS first_tx_p90,
       percentile("first_fetched_to_last_tx_us", 90) AS last_tx_p90,
       percentile("completed_to_last_tx_us", 90) AS completed_to_last_tx_p90,
       percentile("created_bank_to_replay_start_us", 90) AS bank_to_replay_p90,
       percentile("replay_start_to_first_tx_us", 90) AS replay_to_first_tx_p90,
       percentile("frozen_to_processed_us", 90) AS frozen_to_processed_p90,
       percentile("processed_to_confirmed_us", 90) AS proc_to_conf_p90,
       count("slot")
FROM "slot-geyser-latency" WHERE time > now() - 1h AND "first_fetched_to_last_tx_us" >= 0
```

**3. Plugin cost** (run once per measurement: `geyser-notify-account-update`, `geyser-notify-slot-status`, `geyser-notify-deshred-transaction`). An empty result means the plugin doesn't receive that kind of event.
```sql
SELECT sum("total_us")/sum("count") AS avg_us, max("max_us"), sum("count")
FROM "geyser-notify-account-update" WHERE time > now() - 1h
```

**4. Queue, drop and idle health**
```sql
SELECT max("channel_len"), sum("num_packets_dropped"), max("fetch_max_batch_us"), sum("fetch_elapsed_us"), sum("fetch_idle_us")
FROM "shred_fetch_receiver" WHERE time > now() - 1h

SELECT sum("overflow_shreds") FROM "shred_fetch" WHERE time > now() - 1h

SELECT sum("num_retransmit_stage_overflow_shreds"), sum("recv_micros"), sum("recv_wait_micros"), sum("sigverify_micros"), sum("resign_micros")
FROM "shred_sigverify" WHERE time > now() - 1h

SELECT sum("shred_receiver_elapsed_us"), sum("shred_receiver_wait_us"), sum("shred_deserialize_elapsed_us"), sum("blockstore_insert_elapsed_us")
FROM "recv-window-insert-shreds" WHERE time > now() - 1h

SELECT max("max_receiver_len") FROM "entry-notifier-service-timing" WHERE time > now() - 1h
SELECT max("max_receiver_len") FROM "transaction-status-service-timing" WHERE time > now() - 1h
```

**5. Host facts** (from the node): `nproc`; `lscpu | head -20`; the validator command line, especially `--tvu-*` flags (`ps -o args= -C agave-validator`); which geyser plugin(s) are loaded and whether they handle deshred transactions, account updates and transactions.

If the first hour has too few samples, rerun with `AGAVE_PIPELINE_LATENCY_SLOT_SAMPLE=1`.

## SUPERSEDED: Phase 2 baseline results, pulls #1/#2 (2026-09-28 ~16:30; NOT filtered by host)
> **Do not use the multi-node parts of this section.** They were summed over every node that reports to the shared database. The filtered rerun is in "Phase 2 baseline results, pull #3" below; it contradicts several conclusions here (resign was NOT 97% of sigverify busy time, and per-batch fetch time is 2.7ms, not 67µs).
Note: the Grafana table dropped the first selected column of every query, so a query's first field was not available; that is why some fields below are missing. Put a throwaway first column in future queries (e.g. `SELECT count("x"), ...`) or use `influx -format csv`. An earlier pull at 16:12 gave somewhat higher values (total p90 2.7ms, p99 4.7ms), so numbers move by tens of percent between windows.

**Deshred path (from the fetch modifier, last arriving shred of each data set), µs**
| | p50 | p90 | p99 (mean of 10s windows) | max |
|---|---|---|---|---|
| total | 1074 | 1564 | 3060 | 386789 |
| fetch → sigverify done | | 904 | 1911 | 18675 |
| sigverify → insert done | | 502 | 1262 | **386107** |
| insert → service dequeue | | 14 | | 1809 |
| dequeue → entries loaded (RocksDB re-read) | | 151 | | 5182 |
| loaded → notified | | 228 | | 1339 |
| data-set arrival spread | | 8587 | | |

115,610 data sets tracked, 21,767 untracked (16%; recovered/repaired last shred or missing timestamps, so the tail is probably understated).

**Stage ranking by p90 contribution:** fetch→sigverify 50%, sigverify→insert 28%, notify 13%, RocksDB re-read 8%, channel 1%.

**Sigverify thread (sums over the window):** `recv_wait` 3064s (idle), `sigverify` (ed25519 verify) 172s, `resign` (retransmit-signature check + resign) **6152s = 97% of its busy time, 36x the verify cost**. Caveat: these three add up to more than 1h of a single thread's wall time, so the query window or series is not what we assumed (multiple reporters? longer window?). Ratios within the datapoint are still valid.

**Window insert thread:** waiting 3373s, deserialize 7s, blockstore insert 209s. Not saturated.

**Receive:** 53.7M batches, 59.5% full (64 packets); mean time per batch call = 3577s/53.7M = 67µs, so the 5ms coalesce window is not costing us on average (independent of the time window). Longest single call 35.6ms. No dropped packets, no overflow_shreds. Sigverify saw 2.68B packets of which **1.65B (62%) were duplicates**; window insert received 1.002B shreds. Average 49.7 packets per batch.

**Per slot (6716 slots with all stages), p90:** first fetch → first tx notified 16.9ms; first fetch → last tx 262ms; slot completed → last tx 12.0ms; created bank → replay start 153µs; replay start → first tx 5.2ms; frozen → Processed 44µs; Processed → Confirmed 205ms (consensus, not ours).

**Plugin / queues:** account-update callbacks: 42.2M calls, worst 10.6ms, average not captured. Slot-status and deshred plugin timings not yet collected. Peak queue lengths: entry notifier 226, tx status 525.

**Conclusions and re-ranking of Phase 3**
1. The pipeline after the fetch modifier is already fast (p90 1.6ms). The big-ticket item is sigverify, and inside it the resign step (97% of the thread's busy time, in the critical path before shreds reach window insert). Candidate 2 becomes first: shrink or decouple resign.
2. Possible no-code experiment: `--tvu-shred-sigverify-threads` (rayon pool size). The CLI default is `get_thread_count()` = `num_cpus / 2` (min 1), from `rayon-threadlimit/src/lib.rs`; the `2` in `validator.rs` is only the `ValidatorConfig` struct default. Do not raise it blindly: first check from the filtered `shred_sigverify` query how busy the driver thread is (`elapsed_micros` vs the window) and the mean iteration time (`elapsed_micros / num_iters`). A pool that is already half the cores, driven by one thread that waits on a barrier for every dedup / verify / resign step, may be limited by per-iteration synchronisation rather than by CPU. Try one change at a time, in both directions, and compare `fetch_to_sigverify_p90/p99_us`.
3. The single 386ms outlier is in sigverify → insert (window insert / blockstore side), not sigverify. Investigate with `blockstore-insert-shreds` (write_batch, insert_lock) around that time.
4. Candidate 1 (coalesce) is deprioritized: mean per-batch time is 67µs. Candidate 4 (RocksDB re-read, 151µs p90) is now low value.
5. 62% duplicate packets: find out where they come from (extra feeds?). Each still costs receive, fetch-modifier and channel work before dedup.
6. Executed-tx and slot-status paths are not bottlenecks at this point.

## Phase 2 baseline results, pull #3 (test node only, host-filtered, 2026-09-28 17:27-17:32, last 1h)
All values from queries in `docs/perf/baseline-queries.md`. Window = 1h (1795 two-second reports). Units µs unless noted.

**Deshred path (from the fetch modifier, last arriving shred of each data set)**
| | p50 | p90 | p99 | max |
|---|---|---|---|---|
| total | 1071 | 1572 | 3194 | 370979 |
| fetch → sigverify done | | 912 | 2053 | 82329 |
| sigverify → insert done | | 493 | 1236 | 370156 |
| insert → service dequeue | | 14 | | 2177 |
| dequeue → entries loaded (RocksDB re-read) | | 155 | 357 | 5251 |
| loaded → notified | | 244 | 381 | 1339 |
| data-set arrival spread | | 8134 | | |
132,317 data sets tracked, 25,466 untracked (16%). Stage shares of the p90 sum: fetch→sigverify 50%, sigverify→insert 27%, notify 13%, RocksDB re-read 8.5%, channel 1%.

**Slow windows (total_max > 20ms): 6 in the hour.** 4 are in fetch→sigverify (19-82ms), 2 are consecutive 10s windows in sigverify→insert (33ms, then 370ms). Blockstore side: worst 2s report spent 252ms in inserts, worst `write_batch` 76ms, worst `shred_recovery` 74ms, `insert_lock` max 112µs (no lock contention). The 370ms event is a stall of the single window-insert thread (a slow insert plus queueing behind it).

**Receive (`shred_fetch_receiver`)**: 70.1M packets (19.5k pps), 1.31M batches (366/s), 53.4 packets/batch, 67.8% full. **Mean time per batch call = 3578s / 1.31M = 2.72ms** (so the 5ms coalesce window is active: full batches wait for fill, 32% of batches hit the deadline). Longest call 86ms. No drops, channel len max 3, fetch thread never idle. **This 2.7ms is NOT included in `shred-geyser-latency`** (which starts at the modifier); expected added delay for a random shred is about half of it, ~1.4ms on average.

**Sigverify (1 driver thread)**: 1.29M iterations, 1.016 batches per iteration (~54 packets each). Idle (`recv_wait`) 2997s = 83%; busy `elapsed_micros` 593s = **16.5%**. Per iteration mean 459µs = verify 150µs + resign 128µs + other 181µs (dedup, bank_forks read, extract, channel sends; not yet broken down). Packets 70.06M, duplicates 43.8M (**62.5%**), survivors 24.5M = shreds seen by window insert (24.53M).
**Window insert (1 thread)**: 1.29M runs, 19 shreds/run, idle 93%; per run: deserialize 6.4µs + blockstore insert 188µs (≈10µs/shred).
**Queues**: entry notifier peak 297, tx status peak 514.

**Per slot (6708 slots with all stages), p50/p90**: first fetch → first tx notified 3.7ms / 20.0ms; first fetch → last tx 269ms (p90; ≈ slot duration, ~13.4k slots/h); slot completed → last tx **17.3ms**; created bank → replay start 158µs; replay start → first tx 3.7ms; frozen → Processed 45µs; Processed → Confirmed 202ms (consensus).
**Plugin cost**: account updates 47.2M calls, mean **2.0µs**, worst 6.2ms (94.8s per hour in total, ~2.6% of one core). Slot-status and deshred plugin timings still not collected.

**Conclusions / Phase 3 order (replaces the earlier ranking)**
1. **Receiver batching (S1): mean 2.7ms per batch, ~1.4ms expected added delay, invisible to the tracker.** Comparable to the whole measured pipeline (p50 1.07ms). Needs (a) a per-batch fetch-time histogram so before/after can be measured, (b) a configurable coalesce window, then A/B 5ms / 2ms / 1ms / 0.5ms. Risk: smaller batches mean more sigverify iterations, so do (2) as well.
2. **Sigverify per-iteration overhead: 459µs per iteration for ~54 packets, and only 16.5% utilised.** Latency is driven by per-iteration fixed cost (three rayon `install` barriers for dedup / verify / resign plus other serial work), not by throughput. Do NOT add threads. Break the 181µs "other" down, then fuse the passes into one or run small batches serially. Expected: fetch→sigverify p90 0.9ms → ~0.4ms.
3. **Window insert per-run fixed cost: 188µs per 19-shred run.** Need the breakdown sums from `blockstore-insert-shreds`.
4. **Tail**: 370ms stall from a slow blockstore insert (write_batch up to 76ms, recovery up to 74ms on the insert thread). Rare (2 windows/hour). Correlate with RocksDB compaction; consider moving Reed-Solomon recovery off the critical insert path.
5. **Executed-tx path**: slot completed → last tx 17.3ms p90 is ~10x the deshred path; get TSS breakdown (`transaction-status-service-timing`) first.
6. **62.5% duplicate packets (12.2k pps)**: ask where they come from (extra feeds?). Each duplicate still costs receive + modifier + channel + dedup.
7. Low value now: RocksDB re-read (155µs p90), deshred notify (244µs), account-update plugin (2µs mean).

**Still to collect**: `blockstore-insert-shreds` per-phase sums; `transaction-status-service-timing` sums; `modifier_elapsed_us` for `shred_fetch_receiver`; `geyser-notify-slot-status` and `geyser-notify-deshred-transaction`; timestamps of `blockstore-insert-shreds` windows with `total_elapsed_us` > 100ms.

## Progress Log
| Date | Step | Result | Commit |
|---|---|---|---|
| 2026-09-28 | Phase 0: plan created, pipeline mapped, doc committed | — | (this commit) |
| 2026-09-28 | Phase 1a: busy/wait split of shred pipeline metrics | compiles, not yet deployed | 03293d1c3a |
| 2026-09-28 | Phase 1b (deshred path): sampled end-to-end latency tracker, 6 unit tests pass | compiles, not yet deployed | c15c4ca3c3 |
| 2026-09-28 | Phase 1c: plugin callback timings (account update, slot status, deshred tx) | 20 geyser-manager tests pass, not yet deployed | ebb4382aa6 |
| 2026-09-28 | Phase 1b (per-slot): `slot-geyser-latency` covering executed txs and slot status; 5 new unit tests | compiles, 11 tracker + 20 geyser tests pass, not yet deployed | bf617caa77 |
| 2026-09-28 | Phase 2 baseline pulls #1/#2 (unfiltered, multi-node) | superseded, see pull #3 | — |
| 2026-09-28 | Phase 2 baseline pull #3 (test node only, host-filtered) | deshred p50 1.07ms / p90 1.57ms / p99 3.19ms; receiver batching 2.7ms/batch (not in tracker); sigverify 16.5% busy with 459µs per ~54-packet iteration; window insert 7% busy; one 370ms stall | — |

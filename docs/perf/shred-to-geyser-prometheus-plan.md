# Shred → Geyser Latency: Prometheus Instrumentation Plan

> **How to resume:** read this whole doc, find the latest row in the Progress Log at
> the bottom, and continue from there. After each step, append a row (date / step /
> result / commit).

## Context

The goal is to reduce latency from shred receipt to (a) Geyser deshred-transaction
notifications and (b) executed-transaction / account-update / slot-status notifications,
which drives confirmation timing for downstream consumers. The pipeline spans many
threads and channels (UDP receive → sigverify → blockstore insert → completed-data-sets
→ deshred notify, and separately replay → execution → commit/account-notify →
tx-status-notify → slot-status-notify), so before touching any code we need per-stage
timing data to know which stage actually dominates p90/p99 latency.

Requirement: metrics must be served on a **Prometheus scrape endpoint**, not through
Solana's built-in `datapoint!`/InfluxDB pipeline (`solana-metrics` crate). A prior,
unrelated side-branch (`custom-pipeline`) did similar work using `datapoint!`/InfluxDB;
per explicit instruction, this plan does **not** port that branch's design — metric
names, structures and the per-slot raw-row approach are designed fresh here, suited to
how Prometheus actually works (server-side aggregation over bucketed histograms, not
client-side percentile pre-computation).

Confirmed facts driving this plan:
- No `prometheus` (or `metrics-exporter-prometheus`) crate exists anywhere in the
  dependency tree today — this is greenfield. `prometheus = "0.14.0"` is available on
  crates.io.
- `hyper` 0.14.32 is already resolved in `Cargo.lock` (pulled in transitively via
  `jsonrpc-http-server`, used by the JSON-RPC service), but no crate declares it as a
  direct dependency today — only `hyper-util` is a direct workspace dependency. Adding
  `hyper = "0.14"` as a direct dependency does not change the resolved version.
- `tokio = "1.53.1"` is already a workspace dependency.
- The admin-RPC and JSON-RPC services already establish the pattern of "dedicated OS
  thread + small dedicated Tokio runtime + server `.serve()` loop + registration with
  `validator_exit` for clean shutdown" (`rpc/src/rpc_service.rs`,
  `validator/src/admin_rpc_service.rs`) — the new metrics server follows the same
  pattern instead of introducing a new async stack.
- The pipeline code itself (fetch → sigverify → window insert → completed-data-sets →
  replay → execution → notifiers) is byte-for-byte unchanged between the v4.3.0-rc.1
  fork point (`2e10d67f90`) and this branch's tip, so the stage map and file:line
  references below are accurate for this branch.

Decisions made:
- HTTP stack: **hyper 0.14 + a small dedicated Tokio runtime**, mirroring
  `admin_rpc_service.rs`'s existing pattern — no new HTTP crate.
- Registry/metric definitions live in a **new module inside the existing `metrics`
  crate** (`solana-metrics`), not a new workspace crate.
- **No per-slot raw-row dumps.** Aggregate histograms only; outlier investigation
  relies on histogram `p99`/`max` plus existing logs. No ring buffer, no side-channel.
- **All durations are in microseconds** (`_us` metric suffix, `f64` values from
  `elapsed().as_micros()`), a deliberate deviation from Prometheus's usual
  base-unit-in-seconds convention, per explicit requirement.

---

## 1. Pipeline stage map (for picking instrumentation points)

```
UDP tvu sockets ×N ──► solRcvrShredNN ──► EvictingSender(65536) ──► solTvuPktMod ─┐
UDP repair socket  ──► solRcvrShredRep00 ──► EvictingSender(65536) ──────────────┤ fetch_sender(65536)
                                                                                  ▼
                                        solShredVerifr (+ rayon solSvrfyShredNN)
                                                                                  ▼ verified_sender (unbounded)
                                      solWinInsert (+ pool ≤8 solWinInsertNN)
                                        ├─ Blockstore::insert_shreds_at_location_handle_duplicate
                                        ├─ send_signals → ReplayStage
                                        └─ completed_data_sets(100_000)
                                                                                  ▼
                              solComplDataSet: read entries, notify_deshred_transaction (inline)

ReplayStage (solReplayStage/solReplayFork) → confirm_slot → unified scheduler → execute_batch
   ├─ commit → accounts notify INLINE (accounts-db geyser_plugin_utils)
   └─ tx clone → unbounded → solTxStatusWrtr → notify_transaction
   → freeze → BankNotification → slot status Processed/Confirmed/Rooted
```

| # | Stage | Key code (file:line) |
|---|---|---|
| S1 | UDP receive | `streamer/src/streamer.rs` recv loop; `streamer/src/recvmmsg.rs`; `streamer/src/packet.rs` (`recv_from_coalesce`) |
| S2 | Fetch filter/dedup | `core/src/shred_fetch_stage.rs` `modify_packets`; `ledger/src/shred/filter.rs` |
| S3 | Sigverify + dedup + resign | `turbine/src/sigverify_shreds.rs:142` `run_shred_sigverify` |
| S4 | Retransmit (parallel) | `turbine/src/retransmit_stage.rs:321` `retransmit` |
| S5 | Window insert | `core/src/window_service.rs:219` `run_insert`; `ledger/src/blockstore.rs` `do_insert_shreds` |
| S6 | Completed data sets → deshred geyser notify | `core/src/completed_data_sets_service.rs:120` (main loop), `:236` `notify_deshred_transactions_for_completed_data_set`, `:282` call site; `geyser-plugin-manager/src/deshred_transaction_notifier.rs` |
| S7 | Replay + entry load | `core/src/replay_stage.rs:3775` `replay_active_banks`; `ledger/src/blockstore_processor.rs:1360` `confirm_slot` |
| S8 | Execution + account notify | `runtime/src/transaction_execution.rs:57` `execute_batch`; `accounts-db/src/accounts_db/geyser_plugin_utils.rs` |
| S9 | Tx status → geyser | `rpc/src/transaction_status_service.rs:74` (main loop); `geyser-plugin-manager/src/transaction_notifier.rs` |
| S10 | Slot status | `geyser-plugin-manager/src/slot_status_notifier.rs:15-50` |
| S11 | Validator lifecycle (for wiring the new server) | `core/src/validator.rs` — `Validator` struct :683-723, `new_with_exit` (rpc wiring ~:1267), `join` (~:1975-1990) |

---

## 2. Phase 0 — Prometheus plumbing (metrics crate + HTTP endpoint + wiring)

Goal: a working `/metrics` endpoint with zero pipeline instrumentation yet (smoke test),
gated behind a flag that defaults to off.

1. **Dependency**: add `prometheus`, `hyper = "0.14"` to `[workspace.dependencies]` in
   the root `Cargo.toml`. Add `prometheus`, `hyper`, `tokio` (with `rt-multi-thread`) as
   dependencies of `metrics/Cargo.toml`.
2. **New module `metrics/src/prometheus_metrics.rs`**: dedicated `Registry`
   (`LazyLock<prometheus::Registry>`), lazily-registered `Histogram`/`HistogramVec`/
   `IntCounter`/`Gauge` statics, `pub fn gather_text() -> String` via `TextEncoder`.
3. **New module `metrics/src/prometheus_server.rs`**: `MetricsServer` struct — dedicated
   thread + small dedicated Tokio runtime + `hyper::Server`, serves `GET /metrics`,
   registers a shutdown callback with `validator_exit`, exposes `join(self)`.
4. **CLI flag**: `--metrics-listen-address <HOST:PORT>` (modeled on `rpc_bind_address`)
   → `ValidatorConfig.metrics_listen_addr: Option<SocketAddr>`. Unset by default.
5. **Wire into `Validator`**: `metrics_server: Option<MetricsServer>` field, started in
   `new_with_exit`, joined in `Validator::join`.
6. **Smoke test**: `curl 127.0.0.1:<port>/metrics` returns valid Prometheus exposition
   text.

---

## 3. Phase 1 — Instrument the pipeline

**All durations are measured and observed in microseconds.** No buffering, no manual
report-interval timers, no client-side percentile math: Prometheus computes percentiles
server-side from the bucket counts at query time.

Suggested bucket boundaries (microseconds), covering ~10µs–500ms range:
`[10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 500_000]`.

### 3a. Shred path

`agave_shred_stage_duration_us{stage=...}`:

| `stage` value | Meaning | Instrumentation point |
|---|---|---|
| `receive` | UDP socket receive (per batch) | `streamer/src/streamer.rs` recv loop / `recvmmsg.rs` |
| `deserialize` | Parsing raw packets into shred structures | `core/src/shred_fetch_stage.rs` `modify_packets` / `ledger/src/shred` parsing |
| `dedup` | Duplicate-shred elimination | `turbine/src/sigverify_shreds.rs:142` dedup step inside `run_shred_sigverify` |
| `filter` | Discarding invalid/out-of-window packets | `ledger/src/shred/filter.rs` `should_discard_packet` |
| `sign` | Signature verification (ed25519) | `turbine/src/sigverify_shreds.rs:142` verify step |
| `retransmit` | Forwarding shred onward to other validators (assumed to be what "refetch" refers to — **flag if something else was meant, e.g. a repair re-request path**) | `turbine/src/retransmit_stage.rs:321` `retransmit` |

Also: `agave_shred_fetch_queue_length` (Gauge, `fetch_sender`/`verified_sender` channel
length), `agave_shred_packets_dropped_total` (Counter).

### 3b. Blockstore store

`agave_blockstore_store_duration_us{phase=total|insert_shreds|write_batch|recovery|insert_lock|commit_working_sets}`
at `core/src/window_service.rs:219` `run_insert` / `ledger/src/blockstore.rs`
`do_insert_shreds`.

### 3c. Replay

`agave_replay_stage_duration_us{stage=read_blockstore|collect_entries|execute|commit}`
at `replay_stage.rs:3775`, `blockstore_processor.rs:1360`,
`transaction_execution.rs:57`, bank commit path.

`agave_geyser_notify_duration_us{notifier=account_update|slot_status|deshred_transaction|transaction}`
at `accounts_update_notifier.rs`, `slot_status_notifier.rs:15-50`,
`deshred_transaction_notifier.rs`, `transaction_status_service.rs:74`.

### 3d. End-to-end

`agave_end_to_end_duration_us{path=deshred|executed_tx}`:
- `deshred`: first shred received for a data set → `notify_deshred_transaction` returns.
- `executed_tx`: first shred received for a slot → last `notify_transaction` for that slot.

Implementation: `DashMap<(Slot, u32), Instant>` per checkpoint (deshred path, first
shred per FEC set only) and `DashMap<Slot, Instant>` for slot-start (executed-tx path),
pruned by slot age. Every transition calls `.observe()` directly.

---

## 4. Phase 2 — Baseline

Step-by-step deployment/collection instructions are in
`docs/perf/prometheus-deployment.md`. Summary below.

Each validator exposes its own `/metrics` endpoint, so there's no need to filter by
host in queries (unlike a shared InfluxDB sink feeding many nodes).

- Scrape for at least 1 hour at normal load.
- Example PromQL (results are in **microseconds**):
  - `histogram_quantile(0.90, sum(rate(agave_shred_stage_duration_us_bucket[5m])) by (le, stage))`
  - `histogram_quantile(0.90, sum(rate(agave_blockstore_store_duration_us_bucket[5m])) by (le, phase))`
  - `histogram_quantile(0.90, sum(rate(agave_replay_stage_duration_us_bucket[5m])) by (le, stage))`
  - `histogram_quantile(0.90, sum(rate(agave_geyser_notify_duration_us_bucket[5m])) by (le, notifier))`
  - `histogram_quantile(0.90, sum(rate(agave_end_to_end_duration_us_bucket[5m])) by (le, path))`
  - `rate(agave_shred_packets_dropped_total[5m])`, `max_over_time(agave_shred_fetch_queue_length[5m])`
- Record host facts: CPU model/cores, NIC/IRQ affinity, `--tvu-receive-threads`,
  `--tvu-shred-sigverify-threads`, geyser plugins loaded.
- Rank stages by p90 contribution. **Do not assume the prior branch's numbers
  transfer** — re-measure from scratch.

---

## 5. Phase 3 — Optimization

Deferred until Phase 2 produces a ranking. Candidate areas (not pre-ordered):
- S1 receive coalescing window vs. added latency.
- S3 sigverify per-iteration fixed cost vs. batch size / thread count.
- S5 window-insert batch size and Reed-Solomon recovery cost.
- S6 removing the RocksDB re-read in `CompletedDataSetsService` if it shows up.
- S7/S8 replay wake-up latency and execution-thread account-notify cost.
- S9/S10 notifier hop latency if plugins or channel depth show up as significant.

One change at a time, gated by the same PromQL queries before/after, keep only if p90
improves and drop/error counters don't regress.

---

## 6. Verification

- `cargo check -p solana-metrics -p solana-core -p solana-turbine -p solana-ledger -p solana-geyser-plugin-manager -p solana-rpc -p agave-validator`.
- Unit tests: `prometheus_metrics.rs` registration/encode test; existing relevant suites
  (`window_service`, `sigverify_shreds`, `completed_data_sets`) still pass.
- Manual: start the validator with `--metrics-listen-address 127.0.0.1:9101`, confirm
  `curl 127.0.0.1:9101/metrics` exposes the new metric families with non-zero counts.
- Correctness: no regression in existing drop/error counters; instrumentation overhead
  stays well under 1% CPU on the busiest threads (sigverify driver, window-insert
  thread).
- Confirm the server defaults to off and adds no measurable overhead when unset.

---

## Implementation notes (Phase 0 + Phase 1, as built)

- **HTTP stack**: `metrics/src/prometheus_server.rs`, `MetricsServer` — dedicated
  `solPromMetrics` thread + a 1-worker dedicated Tokio runtime + `hyper::Server`,
  `GET /metrics` only (404 otherwise). Shutdown via a clonable
  `MetricsServerCloseHandle` (idempotent, safe to call from both a `validator_exit`
  callback and an explicit `join()`), mirroring `JsonRpcService`'s `CloseHandle`
  pattern in `rpc/src/rpc_service.rs`.
- **Registry**: `metrics/src/prometheus_metrics.rs` — a dedicated
  `prometheus::Registry` (not the crate's global default), plus
  `register_histogram[_vec]`/`register_int_counter[_vec]`/`register_gauge[_vec]`
  helpers and `gather_text()` (Prometheus text exposition format).
- **Metric definitions**: `metrics/src/pipeline_metrics.rs` — all the
  `agave_*_duration_us` `HistogramVec`s and the two auxiliary metrics
  (`agave_shred_fetch_queue_length` Gauge**Vec**, `agave_shred_packets_dropped_total`
  Counter) described in section 3 above.
- **End-to-end tracking**: `metrics/src/pipeline_latency.rs` —
  `DeshredLatencyTracker` (`DashMap<(slot, fec_set_index), Instant>`) and
  `ExecutedTxLatencyTracker` (`DashMap<slot, Instant>`), both pruned by slot age
  (64 slots) only when a new max slot is seen (not on every mark, to bound
  overhead). No sample buffering anywhere — every stage transition calls
  `.observe()` directly on the histogram at the moment the duration is known.
- **CLI**: `--metrics-listen-address <HOST:PORT>` (validated with
  `solana_net_utils::is_host_port`), off by default. Wired through
  `ValidatorConfig.metrics_listen_addr` -> `Validator.metrics_server` exactly like
  `rpc_addrs` -> `json_rpc_service`.

**Instrumentation call sites actually used** (some deviate from the original
per-file guesses in section 1, based on what the code turned out to look like):
- `receive`: `streamer/src/streamer.rs` `recv_loop`, gated on
  `stats.name.starts_with("shred_fetch")` since `streamer::receiver` is shared by
  TPU fetch, repair and shred paths and would otherwise mix unrelated traffic in.
- `deserialize`: `core/src/window_service.rs` `run_insert`, timing
  `Shred::new_from_serialized_shred` (not in the fetch stage — that's where shreds
  actually get deserialized into owned `Shred`s).
- `dedup`, `sign`: `turbine/src/sigverify_shreds.rs` `run_shred_sigverify` — `sign`
  covers both `verify_packets` (ed25519) and the retransmitter verify/resign block,
  timed as one span.
- `filter`: `core/src/shred_fetch_stage.rs` `modify_packets`, same loop where the
  deshred/executed-tx end-to-end trackers get their `mark_started`/
  `mark_slot_started` calls (via `shred::wire::get_shred` + `get_slot`/
  `get_fec_set_index` on the raw wire bytes — cheap, no allocation).
- `retransmit`: `turbine/src/retransmit_stage.rs` `retransmit`, reusing the
  existing `timer_start`/`stats.total_time` `Measure`.
- Blockstore store phases: `core/src/window_service.rs` `run_insert`, by snapshotting
  `BlockstoreInsertionMetrics` fields before/after the
  `insert_shreds_at_location_handle_duplicate` call and observing the deltas —
  avoided touching `ledger/src/blockstore.rs` internals entirely.
- `read_blockstore`, `collect_entries`: `ledger/src/blockstore_processor.rs`
  `confirm_slot` — `collect_entries` wraps the whole per-component loop (which
  internally calls `confirm_slot_entries`/replay), so it is a superset that
  overlaps with `execute`/`commit`, same as `total` overlaps its own sub-phases
  for the blockstore store metric. Accepted as-is; flagged here in case a cleaner
  split is wanted later.
- `execute`, `commit`: `runtime/src/bank.rs`
  `do_load_execute_and_commit_transactions_with_pre_commit_callback` — turned out
  to be the right place, not `runtime/src/transaction_execution.rs::execute_batch`
  (which just calls into this and doesn't itself separate execute from commit).
- Geyser notify (`account_update`, `slot_status`, `deshred_transaction`,
  `transaction`): `geyser-plugin-manager/src/{accounts_update_notifier,
  slot_status_notifier,transaction_notifier}.rs` and
  `core/src/completed_data_sets_service.rs`.
- `executed_tx` end-to-end `mark_tx_notified`: in
  `geyser-plugin-manager/src/transaction_notifier.rs`'s `notify_transaction` (the
  actual point a transaction reaches a geyser plugin), not in
  `rpc/src/transaction_status_service.rs`.
- `agave_shred_fetch_queue_length` is defined but **not yet wired** to a real
  channel-length read — left as a follow-up, not part of the user's required
  metric list.

**Assumption to confirm with the user**: "refetch" in the original shred-path list
was implemented as `retransmit` (forwarding a shred to other validators). Flag if
something else was meant (e.g. a repair re-request path).

**Verification performed**: `cargo check` on every touched crate
(`solana-metrics`, `solana-streamer`, `solana-core`, `solana-turbine`,
`solana-ledger`, `solana-runtime`, `solana-geyser-plugin-manager`, `solana-rpc`,
`agave-validator` bins) all clean. Unit tests run and passing: `solana-metrics`
(27/27, includes 8 new prometheus tests with a live HTTP round-trip),
`solana-core` (`shred_fetch_stage`, `window_service` 4/4, `completed_data_sets_service`
10/10), `solana-turbine` (`sigverify_shreds` 5/5, `retransmit_stage` 2/2),
`solana-geyser-plugin-manager` (18/18, needs `--features agave-unstable-api`),
`solana-ledger` `blockstore_processor` (57/57, 1 pre-existing ignore),
`solana-runtime` (`bank::tests::test_commit_*`, `transaction_execution` 3/3).
`agave-validator run --help` confirmed the new `--metrics-listen-address` flag is
wired end to end.

**Not yet done**: no real validator/testnet run yet (Phase 2 baseline), so no
before/after numbers exist. That is the next step once this is deployed.

**Grafana dashboard**: `docs/perf/grafana-dashboard-shred-geyser.json` — import
into Grafana (Dashboards -> New -> Import), pick a Prometheus datasource that
scrapes this validator's `/metrics` endpoint. Panels: End-to-End latency + rate
by path, Shred Stage duration + throughput, Queue length, Packets dropped,
Blockstore Store phase duration + throughput, Replay Stage duration +
throughput, Geyser Notify duration + throughput. A `$percentile` dropdown
(p50/p90/p95/p99) drives every `histogram_quantile()` panel.

## Progress Log

| Date | Step | Result | Commit |
|---|---|---|---|
| 2026-09-29 | Plan written (this doc), pipeline mapped via 3 parallel Explore agents, confirmed no drift since fork point 2e10d67f90 | — | (uncommitted) |
| 2026-09-29 | Phase 0: Prometheus registry, metric helpers, HTTP `/metrics` server, CLI flag, `Validator` wiring | `solana-metrics` 27/27 tests pass incl. live HTTP round-trip; `agave-validator`/`solana-core` compile clean | (uncommitted) |
| 2026-09-29 | Phase 1: shred-path (receive/deserialize/dedup/filter/sign/retransmit), blockstore-store phases, replay (read/collect/execute/commit), geyser-notify (all 4 notifier types), deshred + executed_tx end-to-end trackers | All touched crates compile; every pre-existing unit test in touched modules still passes (see "Verification performed" above) | (uncommitted) |
| 2026-09-29 | Phase 2 baseline (test node, live `promtool` pull, ~1h range) | See "Phase 2 baseline results" below. **Finding: `receive` (p50 2.99ms/p90 8.21ms/p99 9.82ms) is the single largest shred-path cost and is NOT included in the `deshred` end-to-end number** (the tracker's start point is inside the fetch-filter loop, after receive already happened) -- true socket-to-notify latency is closer to receive + deshred-end-to-end. Everything else (sigverify, blockstore store, replay stages, geyser notify) is comparatively small. Re-ranks Phase 3: coalesce window is priority 1. | — |

## Phase 2 baseline results (2026-09-29, live `promtool` pull, `[1h]` range)

**End-to-end** (us): | path | p50 | p90 | p99 | observations/sec |
|---|---|---|---|---|
| deshred | 2562 | 9181 | 32086 | 67.6 |
| executed_tx | 132161 | 426654 | 492915 | 3803 |

**Shred path stage duration** (us), throughput all ~314-340 batches/sec (consistent across stages -- confirms one shared batch per stage as documented):
| stage | p50 | p90 | p99 |
|---|---|---|---|
| receive | 2987 | 8213 | 9822 |
| sign | 267 | 477 | 962 |
| dedup | 147 | 273 | 600 |
| retransmit | 54 | 100 | 469 |
| filter | 12 | 19 | 49 |
| deserialize | 6.6 | 16 | 31 |

Packets dropped: 0. Queue length: empty (not wired, as expected).

**Blockstore store phase duration** (us), throughput ~313.8 batches/sec for all six (confirms all six are observed together per batch):
| phase | p50 | p90 | p99 |
|---|---|---|---|
| total | 129 | 467 | 944 |
| insert_shreds | 59 | 188 | 456 |
| write_batch | 56 | 162 | 204 |
| recovery | 5.5 | 10 | 471 (heavy tail) |
| insert_lock | 5.0 | 9.0 | 9.9 |
| commit_working_sets | 5.0 | 9.0 | 9.9 |

Sub-phase sum roughly tracks `total` at p50/p90 as expected; at p99 the sum exceeds `total` since each phase's p99 is its own independent worst case, not necessarily from the same batch.

**Replay stage duration** (us):
| stage | p50 | p90 | p99 | throughput/sec |
|---|---|---|---|---|
| read_blockstore | 65 | 166 | 405 | 104 (confirm_slot calls) |
| collect_entries | 597 | 1194 | 4934 | 104 (confirm_slot calls) |
| execute | 45 | 626 | 1912 | 3803 (tx batches) |
| commit | 17 | 47 | 206 | 3803 (tx batches) |

execute/commit throughput (~3803/sec) matches the `executed_tx` end-to-end observation rate and the `transaction`/`deshred_transaction` geyser-notify rates almost exactly -- good cross-check that these are all measuring the same underlying tx flow.

**Geyser notify duration** (us) -- all negligible:
| notifier | p50 | p90 | p99 | throughput/sec |
|---|---|---|---|---|
| account_update | 5.1 | 9.1 | 13 | 10195 |
| deshred_transaction | 5.0 | 9.0 | 9.9 | 3803 |
| transaction | 5.8 | 13 | 39 | 3803 |
| slot_status | 9.4 | 18 | 21 | 22.5 |

**Interpretation:**
1. **`receive` (~3-10ms) dominates the shred path** and is bigger than every other shred-path stage combined (sign+dedup+retransmit+filter+deserialize sums to ~487us at p50, ~885us at p90 -- an order of magnitude less than receive alone). This lines up with the known `coalesce = Some(Duration::from_millis(5))` on the shred UDP sockets (`core/src/shred_fetch_stage.rs` `packet_modifier` -> `streamer::receiver`). **Phase 3 priority 1: make the coalesce window configurable and A/B it (0 / 500us / 1ms / 2ms vs. the current 5ms)**, watching `receive` p50/p90/p99, sigverify iteration count/CPU (smaller batches -> more iterations), and packet-drop/overflow counters.
2. **`receive` is not inside the `deshred` end-to-end number.** The deshred tracker's `mark_started` fires inside the fetch-stage filter loop, i.e. after `receive` has already completed for that batch. So the true "wire to Geyser-notified" latency for the deshred path is closer to **receive + deshred end-to-end**: ~5.5ms p50, ~17.4ms p90, ~41.9ms p99 -- not the 2.6/9.2/32ms the `deshred` panel shows on its own. Worth a follow-up: fold `receive` into the tracker's start point, or at minimum always read them together.
3. **`executed_tx`'s huge numbers (132-493ms) are not explained by any per-stage cost measured here** (all replay/execute/commit/geyser-notify costs are microsecond-scale). This metric is dominated by a transaction's *position within its slot* relative to that slot's own duration (~hundreds of ms), not by processing overhead -- optimizing `execute`/`commit`/`collect_entries` further will barely move it. If the real goal is "how long after execution does a tx reach Geyser" rather than "how long after the slot's first shred", a different, narrower metric would be needed.
4. Blockstore `recovery` and `insert_shreds` have a p99 tail (471us, 456us) worth watching but are not currently the dominant cost anywhere.
5. Geyser plugin cost is negligible across all four notifier types, consistent with earlier findings on the unrelated `custom-pipeline` branch -- plugins are not the bottleneck.

### Follow-up instrumentation: TransactionStatusService queue (added 2026-09-29)

Since `execute`/`commit` are microsecond-scale but `executed_tx` end-to-end p90/p99
(427ms/493ms) exceed a single slot's duration, the gap had to be somewhere not yet
measured. The one candidate hop with no prior visibility: the
`crossbeam_channel::unbounded()` channel between transaction commit and
`TransactionStatusService` (`core/src/validator.rs:2958`), which has no
backpressure and, until now, no depth or wait-time metric.

Added:
- `TransactionStatusBatch.enqueued_at: Instant` (`runtime/src/transaction_execution.rs`),
  stamped in `send_transaction_status_batch` at send time, read back in
  `rpc/src/transaction_status_service.rs::write_transaction_status_batch` to observe
  queue wait into `agave_replay_stage_duration_us{stage="tx_status_queue_wait"}`
  (reuses the existing replay-stage metric family rather than a new one, since it's
  directly comparable to `execute`/`commit`).
- `agave_tx_status_queue_length` (Gauge) -- `transaction_status_receiver.len()`,
  sampled once per `solTxStatusWrtr` loop iteration right after a dequeue.

Dashboard: new "Transaction-Status Queue" row (2 panels) added to
`grafana-dashboard-shred-geyser.json` (now version 4). Baseline queries file and
`run-baseline-queries.sh` updated with the corresponding PromQL.

Verification: `cargo check` on `solana-runtime`, `solana-rpc`, `solana-metrics`,
`solana-ledger`, `solana-core`, `agave-validator` bins all clean.
`solana-rpc::transaction_status_service` tests (2/2) and
`solana-runtime::transaction_execution` tests (6/6) pass.

**Re-baseline result (deployed, 2026-09-29):** `tx_status_queue_wait` p50/p90/p99
= 13us/94us/910us, `agave_tx_status_queue_length` = 0. **The tx-status channel
hypothesis is disproven** -- it is not backing up and does not explain the
`executed_tx` tail.

### Follow-up: histogram bucket ceiling was clipping the tail (found + fixed 2026-09-29)

The same re-baseline showed `executed_tx` p99 = exactly `500000` (us) -- suspicious
because that's *exactly* `DURATION_US_BUCKETS`'s old top boundary (500ms).
`histogram_quantile` cannot extrapolate past the highest finite bucket into
`+Inf`, so it silently returns that boundary instead of the true value whenever
the real quantile falls above it -- our own metric definition was clipping the
one number we most wanted to trust.

**Fix:** widened `DURATION_US_BUCKETS` (`metrics/src/prometheus_metrics.rs`) to
add 1s/2s/5s buckets on top of the existing 10us-500ms range. Affects every
histogram in the pipeline (negligible overhead -- a few extra buckets per
label), but matters specifically for `agave_end_to_end_duration_us{path="executed_tx"}`,
which can legitimately span multiple slots.

**Confirmed fixed after redeploy:** `histogram_quantile(0.99, ...{path="executed_tx"}...)`
now returns `493149.89` -- a non-round number, i.e. genuinely interpolated within
real bucket data rather than clipped. This also matches the very first baseline
pull's p99 (492915us) almost exactly, across three independent measurement
windows. **Conclusion: the ~493ms `executed_tx` p99 tail is real, reproducible,
and not explained by the tx-status queue, execute, commit, read_blockstore, or
collect_entries (all previously ruled out as microsecond-to-low-tens-of-ms
scale).** It is best explained by genuine cross-slot wall-clock variance
(skipped slots, larger blocks, this validator occasionally running behind
realtime) rather than any single instrumented pipeline stage -- there is no
further Phase 2 lead to chase on this specific number with the metrics defined
so far. Phase 3 priority 1 remains the shred-receive coalesce window (see
Section 5), which is the one clearly fixable, high-leverage target the baseline
identified.

### Follow-up instrumentation: execute phase/detail breakdown (added 2026-09-30)

Requested to see if finer detail inside `execute` explains the ~493ms
`executed_tx` tail. It's a long shot given `execute` itself is only ~2ms at p99
(the tail can't be *inside* a span that small), but it's cheap and rules
sub-phases in/out concretely rather than by inference, and specifically checks
for program-cache/JIT-compile cold-start spikes that a single `execute` number
would average away.

Added, reusing Solana's own pre-existing internal accounting rather than new
manual timers (same "snapshot cumulative counters before/after, observe the
delta" pattern used for the blockstore-store phases):
- `agave_execute_phase_duration_us{phase=...}` (`metrics/src/pipeline_metrics.rs`)
  -- from `ExecuteTimings.metrics` (`svm-timings` crate): check, validate_fees,
  load, execute, store, program_cache, filter_executable, collect_balances,
  collect_logs, update_stakes_cache, update_executors, check_block_limits.
- `agave_execute_detail_duration_us{phase=...}` -- from
  `ExecuteTimings.details` (`ExecuteDetailsTimings`): serialize, create_vm,
  execute_inner, deserialize, get_or_create_executor, plus four
  `create_executor_*` sub-phases (register_syscalls, load_elf, verify_code,
  jit_compile) -- the likely hiding place for a cold-program outlier, since
  these should be ~0 for warm/cached programs.
- Wired via a new `ExecuteTimingsSnapshot` helper (module-level, end of
  `runtime/src/bank.rs`) capturing both structs before/after the
  `load_and_execute_transactions` call already timed as `stage="execute"`.
- Dashboard: new "Execute Phase Breakdown" row (2 panels) added to
  `grafana-dashboard-shred-geyser.json` (now version 5). Baseline queries file
  and `run-baseline-queries.sh` updated with the corresponding PromQL.

Verification: `cargo check` on `solana-runtime`, `solana-metrics` clean.
`solana-runtime::transaction_execution` (6/6), `solana-runtime::bank::tests::test_commit_*`
(2/2), `solana-ledger::blockstore_processor` (57/57, 1 pre-existing ignore) all
pass. `agave-validator` bins compile. Not yet deployed/re-baselined with this
addition -- next step is to redeploy and pull the queries in section 7 of
`baseline-queries.md`, watching specifically for any non-trivial mass in
`create_executor_jit_compile` or the other `create_executor_*` phases.

## Phase 3, hypothesis 1: configurable shred coalesce window + sigverify batch size (2026-09-30)

Implements the Phase 3 priority-1 candidate identified from the baseline (the
`receive` stage dominating the shred path, ~3-10ms, consistent with the
previously-hardcoded 5ms coalesce window) plus the related "S3 batch size"
candidate, as two independent CLI flags so both can be A/B tested on a live
node without rebuilding between values.

Added:
- `--shred-fetch-coalesce-us <MICROS>` (default `5000`, matching the prior
  hardcoded behavior exactly) -- controls the `coalesce` window passed to
  `streamer::receiver` for both the TVU and repair shred sockets in
  `core/src/shred_fetch_stage.rs`. `0` disables coalescing (return as soon as
  any packet is available).
- `--shred-sigverify-batch-size <COUNT>` (default `1024`, matching the prior
  hardcoded `SIGVERIFY_SHRED_BATCH_SIZE` constant) -- controls how many packet
  batches `turbine/src/sigverify_shreds.rs::run_shred_sigverify` drains per
  iteration before dedup/verify/resign runs.

Threaded through the same path as the existing `--tvu-shred-sigverify-threads`
flag: CLI arg (`validator/src/commands/run/args.rs`, defaults sourced from
`validator/src/cli.rs::DefaultArgs`) -> parsed in
`validator/src/commands/run/execute.rs` -> `ValidatorConfig` fields
(`core/src/validator.rs`) -> `TvuConfig` fields (`core/src/tvu.rs`) ->
`ShredFetchStage::new`/`spawn_shred_sigverify` parameters.

**Found and fixed in passing:** `solana-local-cluster` (gated behind
`#![cfg(feature = "agave-unstable-api")]` like every other lib crate here, but
nothing else in the workspace depends on it, so it's never unification-checked
by a plain `cargo check -p <other-crate>` the way core/turbine/ledger/etc. are)
had been silently broken since the `metrics_listen_addr` field was added in an
earlier session -- its `safe_clone_config` exhaustively lists every
`ValidatorConfig` field and was missing that one. Fixed by adding
`metrics_listen_addr`, `shred_fetch_coalesce_us`, and
`shred_sigverify_batch_size` to that clone. Lesson for future sessions: after
adding a field to `ValidatorConfig`, explicitly `cargo check -p
solana-local-cluster --features agave-unstable-api` too -- it will not be
caught by checking any other crate.

Verification: `cargo check` across `solana-metrics`, `solana-core`,
`solana-turbine`, `solana-ledger`, `solana-runtime`,
`solana-geyser-plugin-manager`, `solana-rpc`, `solana-streamer`,
`solana-local-cluster` (with `--features agave-unstable-api`), and
`agave-validator` bins all clean together. `solana-turbine::sigverify_shreds`
(5/5), `solana-turbine::retransmit_stage` (2/2), and
`solana-core::window_service` (4/4) tests pass. `agave-validator run --help`
confirms both new flags with the expected defaults.

**Recommended A/B plan for deployment:** baseline first with defaults
unchanged (`--shred-fetch-coalesce-us 5000 --shred-sigverify-batch-size 1024`,
equivalent to omitting both flags), then try
`--shred-fetch-coalesce-us 500` (or `1000`/`2000`) alone first since it's the
higher-confidence lever, watching `agave_shred_stage_duration_us{stage="receive"}`
p50/p90/p99 (expect it to drop substantially), `agave_shred_stage_duration_us{stage="dedup"|"sign"}`
(watch for a rise, since smaller batches mean more, cheaper iterations -- per
the earlier finding that sigverify's cost is dominated by fixed per-iteration
overhead, not per-packet cost, a *smaller* coalesce window could paradoxically
increase total sigverify time if it results in many more iterations), and
`agave_shred_packets_dropped_total`/channel-overflow counters (should stay at
0). Only after that, separately try `--shred-sigverify-batch-size 256` (smaller
than default) to see if it changes the `dedup`/`sign` per-iteration cost curve.
Change one flag at a time per the plan's "one hypothesis per commit/config"
rule, and log before/after p50/p90/p99 in this Progress Log.

## Phase 3, hypothesis 1 result: coalesce window -- tested, no net gain (2026-09-30)

Deployed and A/B'd on the live test node using
`docs/perf/compare-before-after.sh` (added this session; compares two
historical windows via `promtool query instant --time=<ts>` rather than the
`@` PromQL modifier, since this Prometheus version doesn't support `@`).
Baseline: defaults (`coalesce=5000us`, `batch=1024`) at 05:00. Two after-pulls,
both confirming each other: 09:15 with `coalesce=2000us` alone (`batch=1024`
unchanged), and 12:30 with `coalesce=2000us` + `batch=512` together.

**receive improved substantially and reproducibly**, exactly as the mechanism
predicts (smaller coalesce window -> faster batch return, more frequent
batches):
| | before | after (both configs, consistent) |
|---|---|---|
| receive p50/p90/p99 (us) | 2516/7738/9774 | ~1800/4280/4930 |
| receive batches/sec | 346 | ~575 (+66%) |

**But `sigverify_batch_size` 1024->512 turned out to be inert at current
traffic**: comparing the two after-pulls against each other (both at
`coalesce=2000us`, only `batch` differs) shows receive/dedup/sign timings,
iteration rate (~570-576/sec both times) and sigverify busy time (0.1776 vs
0.1785) all within noise of each other. Confirms the Phase 2 baseline's own
finding (sigverify already averages ~1 packet-batch/iteration under normal
load) -- a batch-size *cap* well above what's ever actually queued has nothing
to bind on. Not worth touching again unless traffic grows much heavier.

**The coalesce win doesn't carry through to the outcome metric, and comes with
a reproducible cost**:
| | before | after (both configs, consistent) |
|---|---|---|
| deshred p50/p90 (us) | 2681/9336 | ~2700-2800/9460-9580 (flat) |
| deshred p99 (us) | 26534 | 29449-37037 (worse, noisy but consistently above baseline) |
| sigverify busy time/sec | 0.148 | ~0.178 (**+20%, reproducible**) |
| packets dropped/sec | 0 | 0 |

**Verdict: does not clear the keep bar** (p90 of the outcome metric must
improve and regression guards must not worsen; here p90 is flat and the
sigverify-busy-time guard reproducibly regresses 20%). `receive`'s own
improvement is real but gets absorbed by more frequent sigverify iterations
paying more aggregate fixed per-iteration overhead -- consistent with the
Phase 2 baseline's finding that sigverify cost is dominated by fixed
dispatch/sync cost, not per-packet cost. **Recommendation: revert to the
5000us/1024 defaults** (or try a much smaller trim, e.g. 3500-4000us, if
revisited later) -- not pursuing further for now.

**New finding while analyzing this**: summing the measured shred-path stage
p99s (receive ~9.8ms + dedup ~0.6ms + sign ~1.0ms + blockstore-store total
~0.9ms ~= 12.3ms) falls well short of the actual `deshred` end-to-end p99
(~29-37ms). Per the design note in section 3d, `receive` isn't inside the
`deshred` end-to-end window at all (the tracker starts after receive already
happened), so it doesn't subtract from this gap -- meaning roughly 17-25ms of
the deshred p99 tail is currently unaccounted for by any instrumented stage.
The likely hiding places, per the original Phase 3 candidate list (S6): the
`completed_data_sets` channel wait time between blockstore-insert and
`CompletedDataSetsService` dequeuing it, and/or the RocksDB re-read inside
`recv_completed_data_sets` (`blockstore.get_entries_in_data_block`, which
re-reads and re-deserializes shreds already held in memory by `run_insert`) --
neither is currently instrumented. This becomes Phase 3 hypothesis 2 below.

## Phase 3, hypothesis 2: completed-data-sets channel wait + RocksDB re-read cost (next)

Rationale: see the unaccounted-gap finding immediately above. Plan: instrument
first (per the project's own "measure before optimizing" rule), same pattern
as the tx-status-queue investigation --
1. Channel wait: stamp an `Instant` when `run_insert`
   (`core/src/window_service.rs`) sends a `CompletedDataSetInfo` batch on
   `completed_data_sets_sender`, read it back in
   `CompletedDataSetsService::recv_completed_data_sets`
   (`core/src/completed_data_sets_service.rs`) to observe queue wait, plus a
   channel-length gauge (mirrors `agave_tx_status_queue_length`).
2. RocksDB re-read cost: wrap the `blockstore.get_entries_in_data_block(...)`
   call in `recv_completed_data_sets` with a timer, observed into a new
   `phase="rocksdb_reread"` (or similar) label.
3. Redeploy, re-pull the `deshred` breakdown, and see whether either of these
   two now accounts for the 17-25ms gap. Only attempt the actual fix (S6:
   have `run_insert` pass along the already-deserialized entries instead of
   re-reading them) once the data confirms this is where the time is going --
   not before.

**Implemented (2026-09-30):**
- `agave_deshred_stage_duration_us{stage="rocksdb_reread"|"batch_total"}` --
  `rocksdb_reread` wraps `blockstore.get_entries_in_data_block(...)` per
  completed data set; `batch_total` exposes the service's pre-existing
  `batch_measure` (previously only in the legacy `deshred_geyser_timing`
  datapoint) as a histogram too. Both in `metrics/src/pipeline_metrics.rs` +
  `core/src/completed_data_sets_service.rs`.
- `agave_completed_data_sets_queue_length` (Gauge) --
  `completed_sets_receiver.len()`, sampled once per `solComplDataSet` loop
  iteration right after a successful `recv_timeout`. This channel is bounded
  at 100,000 (`core/src/tvu.rs`), unlike the unbounded tx-status channel, but
  can still build a meaningful backlog well before that ceiling.
- Deliberately did NOT change the `CompletedDataSetsSender`/`Receiver` message
  type to carry a per-message enqueue timestamp (the more direct way to
  measure channel *wait* time, as done for tx-status) -- `CompletedDataSetInfo`
  derives `Eq`/`PartialEq` and has an equality-based test
  (`ledger/src/blockstore/tests.rs`), and three more direct-send test sites in
  `completed_data_sets_service.rs`, making that change meaningfully more
  invasive. The queue-length gauge is the cheaper proxy: combined with
  `batch_total`, it's enough to tell whether this hop has a backlog at all
  before committing to the bigger change.

Dashboard: new "Completed-Data-Sets Channel" row (2 panels) added to
`grafana-dashboard-shred-geyser.json` (now version 6). Baseline queries file
(section 8) and `run-baseline-queries.sh` updated with the corresponding
PromQL.

Verification: `cargo check` on `solana-metrics`, `solana-core`,
`agave-validator` bins, and `solana-local-cluster` (with
`--features agave-unstable-api`, per the lesson above) all clean.
`solana-core::completed_data_sets_service` tests (10/10) pass -- unaffected,
since no message type changed.

**Result (deployed and re-baselined, 2026-09-30): ruled out.**
`rocksdb_reread` p50/p90/p99 = 84/170/385us, `batch_total` p50/p90/p99 =
6.7/332/552us, `agave_completed_data_sets_queue_length` = 0. Both small, no
backlog. **This hop is not where the time is going.**

The `deshred` end-to-end number pulled in the same session was *worse*, not
better: p50/p90/p99 = 2864/9490/48600us (vs the 2681/9336/26534us original
baseline) -- summing every shred-path stage measured so far (dedup ~0.6ms +
sign ~1.0ms + blockstore store ~0.9ms + rocksdb_reread ~0.4ms + batch_total
~0.55ms, all at p99) only accounts for ~3.5ms, leaving **~45ms unexplained at
p99** -- a bigger gap than the ~17-25ms that motivated this hypothesis.

Two things to hold in mind going into hypothesis 3: (a) there's still one
genuinely unmeasured hop on this exact path -- the unbounded
`verified_sender`/`verified_receiver` channel between sigverify and
`window_service::run_insert`, never checked before now; (b) a p99-of-the-sum
can legitimately exceed the sum of independently-computed per-stage p99s if a
traffic burst slows multiple sequential stages *at once* for the same shred --
some of this gap may be that correlation rather than one missing hop, and
hypothesis 3 won't fully resolve that possibility even if it comes back clean.

## Phase 3, hypothesis 3: verified-shreds channel (sigverify -> window_service) wait/backlog

**Implemented (2026-09-30):**
- `agave_verified_shreds_queue_length` (Gauge) -- `verified_receiver.len()`,
  sampled in `core/src/window_service.rs::run_insert` right after
  `recv_timeout` succeeds but *before* the subsequent `try_iter().flatten()`
  drain, so it reflects genuine backlog rather than the emptiness that same
  drain would otherwise leave behind.
- `agave_shred_stage_duration_us{stage="verified_recv_wait"}` -- exposes the
  pre-existing `shred_receiver_elapsed` `Measure` (previously only fed into
  the legacy `WindowServiceMetrics` datapoint) as a histogram too. Reused the
  existing `SHRED_STAGE_DURATION_US` family rather than adding a new one,
  since it's directly comparable to the other shred-path stages. Mostly
  reflects idle wait unless paired with a non-zero queue-length reading.

Verification: `cargo check` on `solana-metrics`, `solana-core`,
`agave-validator` bins, and `solana-local-cluster` (`--features
agave-unstable-api`) all clean. `solana-core::window_service` tests (4/4)
pass.

**Result (deployed and re-baselined, 2026-09-30): also ruled out, but for a
more subtle reason.** `verified_recv_wait` p90 = 4855us -- not small in
absolute terms, comparable to the `receive` stage itself -- but
`agave_verified_shreds_queue_length` = 0 at the same time. Since this queue
has no backlog, `verified_recv_wait` is almost entirely idle time
(`window_service` blocked in `recv_timeout` waiting for sigverify to produce
the next batch) rather than sequential added latency for a specific shred --
that idle wait happens *concurrently with*, not *after*, the tracked shred's
own upstream processing, the same pattern already seen with sigverify itself
(83% idle in the Phase 2 baseline). A consumer with an empty queue when work
arrives isn't adding latency to what arrives. So this hop is cleared too, but
the reasoning is "large-but-not-causal", not "small", which is worth getting
right before ruling out a channel by number size alone in future hops.

## Phase 3, hypothesis 4: is the `deshred` tracker itself trustworthy?

With every discrete pipeline hop on the deshred path now checked (sigverify,
blockstore store, RocksDB re-read, both inter-service channels) and none
explaining the gap, the remaining candidates are (a) burst correlation across
stages (see the note at the end of the hypothesis 2 result), or (b) a flaw in
the tracker itself. (b) is worth checking first since it's cheap and, if true,
would mean the `deshred` p99 number was never trustworthy in the first place --
no amount of further pipeline instrumentation would explain a self-inflicted
measurement artifact.

The specific concern: `DeshredLatencyTracker::mark_started` (`metrics/src/
pipeline_latency.rs`) is keyed by a shred's own `fec_set_index` (from
`shred::wire::get_fec_set_index`, read in the fetch-stage filter loop), while
`mark_notified` is keyed by a completed data set's *starting shred index*
(`completed_data_set_starting_shred_index`, from `ledger/src/blockstore.rs`'s
`update_slot_meta` completion logic, which tracks contiguous *consumed* shred
ranges terminated by a `DATA_COMPLETE_SHRED` flag). These two are assumed
equal (`fec_set_index == data set start index`), and the code that assigns
FEC-set boundaries and the code that assigns data-set-completion boundaries
do share the same `DATA_COMPLETE_SHRED` flag as their terminator, which
supports the assumption in the common case -- but nothing in
`insert_data_shred`/`update_slot_meta` *guarantees* a completed data set's
start always falls on a FEC-set boundary (e.g. after repair fills a gap, or
after a validator joins mid-slot). If they diverge, `mark_notified` either
finds no match (silently dropped observation, not a spike) or, rarely, could
match a stale unrelated entry with the same key by coincidence (a spurious
large or small duration).

**Implemented (2026-09-30):** `agave_deshred_tracking_total{outcome="tracked"|
"untracked"}` (IntCounterVec) -- incremented in `mark_notified`
(`metrics/src/pipeline_latency.rs`) depending on whether a matching
`mark_started` entry was found. A high `untracked` rate directly proves the
`deshred` metric is sampling an unrepresentative subset of data sets rather
than measuring what it claims to; a near-zero rate rules this hypothesis out
too and leaves burst correlation as the remaining explanation.

Verification: `cargo check` on `solana-metrics`, `solana-core`,
`agave-validator` bins, `solana-local-cluster` (`--features
agave-unstable-api`) all clean. `solana-metrics::pipeline_latency` tests
(3/3) pass. Not yet deployed/re-baselined -- next step: pull
`sum(rate(agave_deshred_tracking_total[1h])) by (outcome)` and compute the
untracked fraction.

## Vote/confirmation latency (new, separate from the Phase 3 hypotheses above)

While investigating the `executed_tx` tail, established that slot
*confirmation* is driven by `OptimisticallyConfirmedBankTracker` aggregating
cluster votes -- a subsystem that runs independently of this validator's own
transaction execution/replay, and therefore wasn't measured by anything built
so far (every metric above is on the shred-receive/replay/execute/commit/
notify path, none of it vote-related). Added instrumentation to measure this
directly rather than keep inferring it from replay-side numbers.

**Implemented (2026-10-01):**
- `agave_slot_confirmation_duration_us{stage="created_bank_to_confirmed"|
  "frozen_to_confirmed"}` (`metrics/src/pipeline_metrics.rs`).
- `SlotConfirmationLatencyTracker` (`metrics/src/pipeline_latency.rs`) --
  slot-keyed, stores `created_bank`/`frozen` `Instant`s, observes both deltas
  (whichever start timestamps are present) when `Confirmed` fires. Same
  64-slot-age pruning convention as the other slot-keyed trackers in this
  file; entries are not removed on `Confirmed` (bounded by the same pruning),
  leaving room to add `confirmed_to_rooted` later without redesigning this.
- Wired into `geyser-plugin-manager/src/slot_status_notifier.rs::
  notify_bank_status` (handles `CreatedBank`/`Processed`/`Confirmed`/`Rooted`
  for this notifier), matched on `SlotStatus` and placed *before* the
  function's early-return-if-no-plugins check, so slot confirmation timing is
  recorded even with zero Geyser plugins loaded -- it's a validator-level
  concern, not a plugin one.
- Timestamps are taken at the top of `notify_bank_status`, which is a close
  approximation of (not exactly) the underlying event time -- there's a small
  channel hop (`BankNotification` -> `solOpConfBnkTrk` -> `solBankNotif`)
  between the actual freeze/vote-threshold event and this notifier being
  invoked, analogous to other channel hops already measured elsewhere in this
  plan and found negligible.

Dashboard: new "Slot Confirmation Latency" row (1 full-width panel) added
(now version 9). Baseline queries file (section 11) and
`run-baseline-queries.sh` updated with the corresponding PromQL.

Verification: `cargo check` on `solana-metrics`, `solana-geyser-plugin-manager`,
`solana-core`, `agave-validator` bins, `solana-local-cluster` (`--features
agave-unstable-api`) all clean. `solana-metrics::pipeline_latency` (3 new
tests) and `solana-geyser-plugin-manager::slot_status_notifier` (1/1,
unaffected since no message type changed) pass. Not yet deployed/re-baselined.

**How to read the result once pulled**: if `frozen_to_confirmed` is large
while `created_bank_to_confirmed` is only slightly larger than it, most of
the slot's total time-to-confirm is pure vote propagation/aggregation, not
this validator's own replay speed -- directly relevant to the earlier
`executed_tx` tail discussion (see the Progress Log entries above), since it
would confirm that votes, not transaction processing, dominate how long a
slot takes to be externally recognized as confirmed from this node's
perspective.

### Result (deployed and pulled, 2026-10-01): conclusive -- confirmation is ~96% vote-driven

```
frozen_to_confirmed p90        = 441,885us (441.9ms)
created_bank_to_confirmed p90  = 461,641us (461.6ms)
implied created_bank_to_frozen ~= 19.7ms (this validator's own replay time for the whole slot)
```

The implied `created_bank_to_frozen` (~20ms) matches almost exactly the
independently-measured `collect_entries` p99 (~22ms) from the earlier Phase 3
hypothesis 2 baseline -- two separately-built metrics agreeing is a strong
cross-validation that both are measuring real things, not artifacts.

**Conclusion: ~96% of a slot's time-to-confirm (frozen_to_confirmed /
created_bank_to_confirmed) is vote propagation/aggregation via
`OptimisticallyConfirmedBankTracker`; only ~4% is this validator's own replay.**
This fully resolves the `executed_tx` end-to-end tail discussion earlier in
this doc: block *production* (leader schedule, ~250-400ms cadence) and block
*confirmation* (votes reaching supermajority) are decoupled processes. The
chain keeps producing blocks on schedule regardless of how long any one
slot's votes take to accumulate -- which is exactly why slot cadence stays
stable while this validator's own `executed_tx`/`frozen_to_confirmed` numbers
sit at a roughly constant ~440-460ms. It is not backlog (every queue gauge
checked on the replay path reads ~0), not a pipeline bottleneck (every stage
independently measured is microseconds to low tens-of-ms) -- it is genuinely
how long optimistic confirmation takes on this network right now, now
measured directly instead of inferred.

**Scope implication going forward**: the shred-to-geyser pipeline this whole
plan has been instrumenting and tuning (`deshred` path: receive, sigverify,
blockstore, completed-data-sets; `executed_tx` path: replay, execute, commit,
tx-status) is upstream of and decoupled from vote/confirmation timing.
`notify_deshred_transaction` fires pre-replay, before any voting occurs at
all, and even the `executed_tx` path's own replay/execute/commit stages are
fast and unaffected by vote propagation. Phase 3 hypotheses 1-4 remain the
correct track for improving *this validator's own* notification latency
(deshred and raw execution results); nothing further in this pipeline will
move the vote-confirmation number, since that is gated by a different
subsystem (gossip/vote-transaction propagation, stake distribution, network
topology) outside this project's scope.

### Correction (2026-10-01): the "it's all voting" framing was incomplete

User correctly pushed back: `notify_transaction` (which stops the
`executed_tx` clock) fires at **commit**, not confirmation --
`send_transaction_status_batch` is called synchronously inside
`execute_batch` (`runtime/src/transaction_execution.rs:148`), verified
directly in the code, with zero dependency on voting. So `executed_tx`
(first-shred -> commit) should only reflect replay speed -- which we'd
already measured as fast (`created_bank_to_frozen` ~20ms, implied from the
two slot-confirmation stages above). The ~400+ms in `executed_tx` therefore
could *not* actually be vote-propagation time; it had to be hiding somewhere
between shred arrival and replay starting on the slot, a gap ("replay
wake-up latency," the original plan's Phase 3 candidate #7) that had never
been directly measured -- only inferred around.

**Implemented (2026-10-01):** `agave_slot_confirmation_duration_us{stage=
"first_shred_to_created_bank"}` -- `ExecutedTxLatencyTracker::mark_bank_created`
(`metrics/src/pipeline_latency.rs`) reads (does not remove) the tracker's
existing per-slot start timestamp when `CreatedBank` fires, observing the
delta. Wired into `geyser-plugin-manager/src/slot_status_notifier.rs::
notify_bank_status`'s `CreatedBank` arm, alongside the existing
`SLOT_CONFIRMATION_LATENCY.mark_created_bank` call.

Verification: `cargo check` on `solana-metrics`, `solana-geyser-plugin-manager`,
`solana-core`, `agave-validator` bins, `solana-local-cluster` (`--features
agave-unstable-api`) all clean. `solana-metrics::pipeline_latency` (2 new
tests, 8 total) and `solana-geyser-plugin-manager::slot_status_notifier`
(1/1) pass. Dashboard panel description updated in place (now version 10).
Not yet deployed/re-baselined.

**What to look for once pulled:** if `first_shred_to_created_bank` alone
accounts for most of `executed_tx`'s p90/p99, that confirms the real
bottleneck for this specific metric is replay wake-up/scheduling latency --
not voting (which only governs `frozen_to_confirmed`, a separate, later
stage) and not replay execution itself (which stays fast per
`created_bank_to_frozen`). This would also mean the earlier "~96% vote
propagation" conclusion needs to be read as applying specifically to
*confirmation* timing (`created_bank_to_confirmed`/`frozen_to_confirmed`),
not to `executed_tx`, which is a different span entirely and was always
going to be dominated by whatever happens *before* `created_bank`, not after
`frozen`.

### Result (deployed and pulled, 2026-10-01): `first_shred_to_created_bank` is small -- ruled out, and a math error found along the way

`first_shred_to_created_bank` p90 = 19,288us (19.3ms). Small -- rules out
replay wake-up/scheduling as the explanation for `executed_tx`'s tail.

But this immediately created a contradiction: combined with the earlier
(invalid) `created_bank_to_frozen` estimate (~20ms, from subtracting
`created_bank_to_confirmed` minus `frozen_to_confirmed`), the implied total
"first shred to frozen" was only ~40ms -- yet no transaction can be notified
(and therefore contribute an `executed_tx` observation) after its own slot
freezes, so every `executed_tx` value should be bounded by roughly its slot's
own first-shred-to-frozen time. ~40ms vs. an `executed_tx` p90 of ~400ms+
can't both be true.

**Root cause of the contradiction: invalid percentile arithmetic**, not a
real anomaly. `created_bank_to_frozen` was never measured directly -- it was
inferred by subtracting two *independently-computed* percentiles
(`created_bank_to_confirmed` minus `frozen_to_confirmed`), and
`p90(A) - p90(B) != p90(A - B)` in general. That subtraction should not have
been used to reason about a third quantity.

**Fix: added `first_shred_to_frozen`, a direct measurement** (not composed
from other metrics) -- `ExecutedTxLatencyTracker::mark_bank_frozen`
(`metrics/src/pipeline_latency.rs`), reads (does not remove) the tracker's
existing per-slot start timestamp when `Processed` (bank freeze) fires,
wired into `slot_status_notifier.rs::notify_bank_status`'s `Processed` arm
alongside the existing `SLOT_CONFIRMATION_LATENCY.mark_frozen` call.

Verification: `cargo check` on `solana-metrics`, `solana-geyser-plugin-manager`,
`solana-core`, `agave-validator` bins, `solana-local-cluster` (`--features
agave-unstable-api`) all clean. `solana-metrics::pipeline_latency` (2 new
tests, 10 total) and `solana-geyser-plugin-manager::slot_status_notifier`
(1/1) pass. Dashboard panel description updated in place (now version 11).
Not yet deployed/re-baselined.

**What to look for once pulled:** compare `first_shred_to_frozen` directly
against `executed_tx` (path=executed_tx on the End-to-End panel). If
`first_shred_to_frozen` p90/p99 is itself large (hundreds of ms) and roughly
tracks `executed_tx`, that's the real, validly-measured answer: slots
genuinely take that long to fully replay end-to-end on this validator (still
consistent with the small `first_shred_to_created_bank` and
`created_bank_to_confirmed`-vs-`frozen_to_confirmed` cross-check, just not
with the invalid subtraction). If `first_shred_to_frozen` is small (tens of
ms, matching `first_shred_to_created_bank`) while `executed_tx` stays large,
that's a genuine, currently-unexplained discrepancy worth investigating
further -- possibly pointing at something in the tx-status/notify path we
haven't caught, or a measurement issue specific to `executed_tx`'s own
tracker.

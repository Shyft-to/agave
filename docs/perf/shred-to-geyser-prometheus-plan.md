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
`solana-runtime::transaction_execution` tests (6/6) pass. Not yet deployed/re-baselined
with this addition.

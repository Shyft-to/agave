#!/usr/bin/env bash
# Runs the Phase 2 baseline queries from docs/perf/baseline-queries.md via promtool.
# Usage: ./run-baseline-queries.sh [prometheus_url] [range]
#   prometheus_url defaults to http://localhost:9090
#   range defaults to 1h (edit the RANGE variable below, or pass e.g. 15m if the
#   node hasn't been up for a full hour yet)

set -euo pipefail

PROM_URL="${1:-http://localhost:9090}"
RANGE="${2:-1h}"

run() {
    echo "### $1"
    promtool query instant "$PROM_URL" "$2"
    echo
}

echo "== 1. End-to-end latency =="
run "p50 end-to-end by path"  "histogram_quantile(0.5, sum(rate(agave_end_to_end_duration_us_bucket[$RANGE])) by (le, path))"
run "p90 end-to-end by path"  "histogram_quantile(0.9, sum(rate(agave_end_to_end_duration_us_bucket[$RANGE])) by (le, path))"
run "p99 end-to-end by path"  "histogram_quantile(0.99, sum(rate(agave_end_to_end_duration_us_bucket[$RANGE])) by (le, path))"
run "end-to-end observation rate by path" "sum(rate(agave_end_to_end_duration_us_count[$RANGE])) by (path)"

echo "== 2. Shred path stages =="
run "p50 shred stage duration" "histogram_quantile(0.5, sum(rate(agave_shred_stage_duration_us_bucket[$RANGE])) by (le, stage))"
run "p90 shred stage duration" "histogram_quantile(0.9, sum(rate(agave_shred_stage_duration_us_bucket[$RANGE])) by (le, stage))"
run "p99 shred stage duration" "histogram_quantile(0.99, sum(rate(agave_shred_stage_duration_us_bucket[$RANGE])) by (le, stage))"
run "shred stage throughput"   "sum(rate(agave_shred_stage_duration_us_count[$RANGE])) by (stage)"
run "shred packets dropped/sec" "rate(agave_shred_packets_dropped_total[$RANGE])"
run "shred fetch queue length (not yet wired, expect empty)" "agave_shred_fetch_queue_length"

echo "== 3. Blockstore store =="
run "p50 blockstore store phase duration" "histogram_quantile(0.5, sum(rate(agave_blockstore_store_duration_us_bucket[$RANGE])) by (le, phase))"
run "p90 blockstore store phase duration" "histogram_quantile(0.9, sum(rate(agave_blockstore_store_duration_us_bucket[$RANGE])) by (le, phase))"
run "p99 blockstore store phase duration" "histogram_quantile(0.99, sum(rate(agave_blockstore_store_duration_us_bucket[$RANGE])) by (le, phase))"
run "blockstore store throughput" "sum(rate(agave_blockstore_store_duration_us_count[$RANGE])) by (phase)"

echo "== 4. Replay & execution =="
run "p50 replay stage duration" "histogram_quantile(0.5, sum(rate(agave_replay_stage_duration_us_bucket[$RANGE])) by (le, stage))"
run "p90 replay stage duration" "histogram_quantile(0.9, sum(rate(agave_replay_stage_duration_us_bucket[$RANGE])) by (le, stage))"
run "p99 replay stage duration" "histogram_quantile(0.99, sum(rate(agave_replay_stage_duration_us_bucket[$RANGE])) by (le, stage))"
run "replay stage throughput"   "sum(rate(agave_replay_stage_duration_us_count[$RANGE])) by (stage)"

echo "== 5. Geyser notify cost =="
run "p50 geyser notify duration" "histogram_quantile(0.5, sum(rate(agave_geyser_notify_duration_us_bucket[$RANGE])) by (le, notifier))"
run "p90 geyser notify duration" "histogram_quantile(0.9, sum(rate(agave_geyser_notify_duration_us_bucket[$RANGE])) by (le, notifier))"
run "p99 geyser notify duration" "histogram_quantile(0.99, sum(rate(agave_geyser_notify_duration_us_bucket[$RANGE])) by (le, notifier))"
run "geyser notify throughput"   "sum(rate(agave_geyser_notify_duration_us_count[$RANGE])) by (notifier)"

echo "== 6. Transaction-status queue =="
run "p50 tx-status queue wait" "histogram_quantile(0.5, sum(rate(agave_replay_stage_duration_us_bucket{stage=\"tx_status_queue_wait\"}[$RANGE])) by (le))"
run "p90 tx-status queue wait" "histogram_quantile(0.9, sum(rate(agave_replay_stage_duration_us_bucket{stage=\"tx_status_queue_wait\"}[$RANGE])) by (le))"
run "p99 tx-status queue wait" "histogram_quantile(0.99, sum(rate(agave_replay_stage_duration_us_bucket{stage=\"tx_status_queue_wait\"}[$RANGE])) by (le))"
run "tx-status queue length"   "agave_tx_status_queue_length"

echo "== 7. Execute phase/detail breakdown =="
run "p50 execute phase duration"  "histogram_quantile(0.5, sum(rate(agave_execute_phase_duration_us_bucket[$RANGE])) by (le, phase))"
run "p90 execute phase duration"  "histogram_quantile(0.9, sum(rate(agave_execute_phase_duration_us_bucket[$RANGE])) by (le, phase))"
run "p99 execute phase duration"  "histogram_quantile(0.99, sum(rate(agave_execute_phase_duration_us_bucket[$RANGE])) by (le, phase))"
run "p50 execute detail duration" "histogram_quantile(0.5, sum(rate(agave_execute_detail_duration_us_bucket[$RANGE])) by (le, phase))"
run "p90 execute detail duration" "histogram_quantile(0.9, sum(rate(agave_execute_detail_duration_us_bucket[$RANGE])) by (le, phase))"
run "p99 execute detail duration" "histogram_quantile(0.99, sum(rate(agave_execute_detail_duration_us_bucket[$RANGE])) by (le, phase))"

echo "== 8. Completed-data-sets channel wait + RocksDB re-read =="
run "p50 deshred stage duration" "histogram_quantile(0.5, sum(rate(agave_deshred_stage_duration_us_bucket[$RANGE])) by (le, stage))"
run "p90 deshred stage duration" "histogram_quantile(0.9, sum(rate(agave_deshred_stage_duration_us_bucket[$RANGE])) by (le, stage))"
run "p99 deshred stage duration" "histogram_quantile(0.99, sum(rate(agave_deshred_stage_duration_us_bucket[$RANGE])) by (le, stage))"
run "completed-data-sets queue length" "agave_completed_data_sets_queue_length"

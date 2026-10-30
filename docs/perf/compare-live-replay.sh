#!/usr/bin/env bash
# A/B for the live-replay change (replay reads entries from the window_service
# channel instead of waiting on the blockstore write). Evaluates every query at
# two historical timestamps via `./promtool query instant --time=<ts>`.
#
# Usage: ./compare-live-replay.sh <prometheus_url> <before_ts> <after_ts> [range]
#   before_ts / after_ts: unix timestamps marking the END of each window.
#   range: window averaged over at each timestamp (default 30m). Make sure the
#          window lies entirely inside one build's uptime.
#
# Run from the directory containing your `promtool` binary.
#
# Single-window run (no comparison): pass the same ts for both, or use
#   ./promtool query instant http://localhost:9090 '<query>'

set -euo pipefail

PROM_URL="${1:?usage: $0 <prometheus_url> <before_ts> <after_ts> [range]}"
BEFORE_TS="${2:?missing before_ts}"
AFTER_TS="${3:?missing after_ts}"
RANGE="${4:-30m}"

compare() {
    local label="$1" query_template="$2" query
    query=${query_template//%s/$RANGE}
    echo "### $label"
    echo "  query: $query"
    echo -n "  before: "
    ./promtool query instant --time="$BEFORE_TS" "$PROM_URL" "$query" 2>&1
    echo -n "  after:  "
    ./promtool query instant --time="$AFTER_TS" "$PROM_URL" "$query" 2>&1
    echo
}

# $1 = label prefix, $2 = metric, $3 = label selector (may be empty), $4 = by-clause
quantiles() {
    local name="$1" metric="$2" sel="$3" by="$4" q
    for q in 0.5 0.9 0.99; do
        compare "p$(printf '%.0f' "$(echo "$q * 100" | bc)") $name" \
            "histogram_quantile($q, sum(rate(${metric}_bucket${sel}[%s])) by (le${by}))"
    done
}

echo "== 1. Did replay stop waiting on the blockstore write? =="
quantiles "read_blockstore"  agave_replay_stage_duration_us '{stage="read_blockstore"}' ''
quantiles "collect_entries"  agave_replay_stage_duration_us '{stage="collect_entries"}' ''
quantiles "created_bank_to_frozen" agave_slot_confirmation_duration_us '{stage="created_bank_to_frozen"}' ''
quantiles "first_shred_to_created_bank" agave_slot_confirmation_duration_us '{stage="first_shred_to_created_bank"}' ''

echo "== 2. Is live replay being used? =="
compare "live replay polls/sec by outcome" 'sum(rate(agave_live_replay_entries_total[%s])) by (outcome)'

echo "== 3. End-to-end latency =="
quantiles "end-to-end" agave_end_to_end_duration_us '' ', path'

echo "== 4. Cost added to the insert thread =="
quantiles "blockstore store" agave_blockstore_store_duration_us '' ', phase'
quantiles "verified_recv_wait" agave_shred_stage_duration_us '{stage="verified_recv_wait"}' ''
compare "verified shreds queue length (max)" 'max_over_time(agave_verified_shreds_queue_length[%s])'

echo "== 5. collect_entries phase breakdown =="
quantiles "collect phase" agave_collect_entries_phase_duration_us '' ', phase'

echo "== 6. tx_pipeline decomposition (means; the segments should sum to the total) =="
compare "mean µs per segment" \
    'sum(rate(agave_tx_pipeline_segment_duration_us_sum[%s])) by (segment) / sum(rate(agave_tx_pipeline_segment_duration_us_count[%s])) by (segment)'
compare "mean µs tx_pipeline total" \
    'sum(rate(agave_end_to_end_duration_us_sum{path="tx_pipeline"}[%s])) / sum(rate(agave_end_to_end_duration_us_count{path="tx_pipeline"}[%s]))'
compare "segment coverage of tx_pipeline observations" \
    'sum(rate(agave_tx_pipeline_segment_duration_us_count{segment="fetch_to_data_set_ready"}[%s])) / sum(rate(agave_end_to_end_duration_us_count{path="tx_pipeline"}[%s]))'
quantiles "tx_pipeline segment" agave_tx_pipeline_segment_duration_us '' ', segment'

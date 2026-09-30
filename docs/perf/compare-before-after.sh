#!/usr/bin/env bash
# Compares shred-pipeline metrics between two time windows (e.g. before/after
# changing --shred-fetch-coalesce-us or --shred-sigverify-batch-size and
# redeploying), using `promtool query instant --time=<ts>` to evaluate the
# same query as of two different historical timestamps. (An earlier version
# of this script used PromQL's `@` modifier instead, but that requires
# Prometheus >= 2.33 or --enable-feature=promql-at-modifier; --time is a
# promtool-side flag so it works regardless of server version.)
#
# Usage: ./compare-before-after.sh <prometheus_url> <before_ts> <after_ts> [range]
#   before_ts, after_ts: unix timestamps marking the END of each window
#     (e.g. `date +%s` right before you restart with the new flag value, and
#     again after the new config has run for at least `range`).
#   range: how far back from each timestamp to average over (default 30m).
#
# Example:
#   BEFORE=$(date +%s)              # note this, then wait/deploy
#   # ... restart with --shred-fetch-coalesce-us 1000, let it run 30+ min ...
#   AFTER=$(date +%s)
#   ./compare-before-after.sh http://localhost:9090 $BEFORE $AFTER 30m
#
# Run this from the same directory as your `promtool` binary (it calls
# ./promtool, matching how this host has been running it).

set -euo pipefail

PROM_URL="${1:?usage: $0 <prometheus_url> <before_ts> <after_ts> [range]}"
BEFORE_TS="${2:?missing before_ts}"
AFTER_TS="${3:?missing after_ts}"
RANGE="${4:-30m}"

# Runs one query at each timestamp (via promtool's trailing <time> arg, not
# the `@` modifier) and prints before/after. Errors are shown, not swallowed,
# so a bad query is visible instead of looking like empty data.
compare() {
    local label="$1" query_template="$2" query
    query=$(printf "$query_template" "$RANGE")

    echo "### $label"
    echo "  query: $query"
    echo -n "  before: "
    ./promtool query instant --time="$BEFORE_TS" "$PROM_URL" "$query" 2>&1
    echo -n "  after:  "
    ./promtool query instant --time="$AFTER_TS" "$PROM_URL" "$query" 2>&1
    echo
}

echo "== Shred stage duration (the direct target of these flags) =="
compare "p50 receive"  'histogram_quantile(0.5, sum(rate(agave_shred_stage_duration_us_bucket{stage="receive"}[%s])) by (le))'
compare "p90 receive"  'histogram_quantile(0.9, sum(rate(agave_shred_stage_duration_us_bucket{stage="receive"}[%s])) by (le))'
compare "p99 receive"  'histogram_quantile(0.99, sum(rate(agave_shred_stage_duration_us_bucket{stage="receive"}[%s])) by (le))'
compare "p50 dedup"    'histogram_quantile(0.5, sum(rate(agave_shred_stage_duration_us_bucket{stage="dedup"}[%s])) by (le))'
compare "p90 dedup"    'histogram_quantile(0.9, sum(rate(agave_shred_stage_duration_us_bucket{stage="dedup"}[%s])) by (le))'
compare "p50 sign"     'histogram_quantile(0.5, sum(rate(agave_shred_stage_duration_us_bucket{stage="sign"}[%s])) by (le))'
compare "p90 sign"     'histogram_quantile(0.9, sum(rate(agave_shred_stage_duration_us_bucket{stage="sign"}[%s])) by (le))'
compare "receive batches/sec (iteration cadence)" 'sum(rate(agave_shred_stage_duration_us_count{stage="receive"}[%s]))'
compare "dedup/sign iterations/sec"               'sum(rate(agave_shred_stage_duration_us_count{stage="dedup"}[%s]))'

echo "== Outcome metric (does the change actually help end-to-end?) =="
compare "p50 deshred end-to-end" 'histogram_quantile(0.5, sum(rate(agave_end_to_end_duration_us_bucket{path="deshred"}[%s])) by (le))'
compare "p90 deshred end-to-end" 'histogram_quantile(0.9, sum(rate(agave_end_to_end_duration_us_bucket{path="deshred"}[%s])) by (le))'
compare "p99 deshred end-to-end" 'histogram_quantile(0.99, sum(rate(agave_end_to_end_duration_us_bucket{path="deshred"}[%s])) by (le))'

echo "== Regression guards (must NOT get worse) =="
compare "packets dropped/sec"      'sum(rate(agave_shred_packets_dropped_total[%s]))'
compare "sigverify busy time/sec (proxy for CPU cost)" 'sum(rate(agave_shred_stage_duration_us_sum{stage=~"dedup|sign"}[%s])) / 1e6'

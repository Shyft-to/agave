# Deploying the Prometheus metrics endpoint (Phase 2 setup)

## 1. Build

```sh
cargo build --release -p agave-validator
```

## 2. Run with the metrics endpoint enabled

Add one flag to the existing validator command line (bind to a private/loopback
address unless your Prometheus server is remote and the port is firewalled):

```sh
--metrics-listen-address 127.0.0.1:9101
```

Confirm it's up:

```sh
curl 127.0.0.1:9101/metrics | head -30
```

You should see `agave_shred_stage_duration_us_bucket{...}` etc. once shreds start
flowing. If the port isn't reachable from wherever Prometheus runs, use the node's
private IP instead of `127.0.0.1` and make sure the port is open to that scraper
only (not the public internet).

## 3. Point Prometheus at it

Add a scrape job to `prometheus.yml` (adjust `targets` to the node's actual
address):

```yaml
scrape_configs:
  - job_name: agave-shred-geyser
    scrape_interval: 5s
    static_configs:
      - targets: ["127.0.0.1:9101"]
        labels:
          host_id: my-test-node   # only needed if this Prometheus scrapes multiple nodes
```

Reload/restart Prometheus, then check Status -> Targets shows it as `UP`.

## 4. Import the dashboard

Grafana -> Dashboards -> New -> Import -> upload
`docs/perf/grafana-dashboard-shred-geyser.json`. When prompted, pick the
Prometheus datasource from step 3. The `$percentile` dropdown at the top switches
every latency panel between p50/p90/p95/p99.

## 5. Collect the Phase 2 baseline

- Let it run at least 1 hour at normal load.
- Record, per panel: p50/p90/p99 for each stage/phase/notifier label, plus the
  throughput panels (to catch a stage that's silently not running) and the
  packets-dropped panel (should be ~0).
- Record host facts: CPU model/cores, NIC/IRQ affinity, `--tvu-receive-threads`,
  `--tvu-shred-sigverify-threads`, which Geyser plugins are loaded and which
  event types they subscribe to (account update / slot status / deshred tx / tx).
- Paste the results into the Progress Log in
  `docs/perf/shred-to-geyser-prometheus-plan.md`.
- Rank stages by p90 contribution to (a) the `deshred` end-to-end path and (b)
  the `executed_tx` end-to-end path. Phase 3 optimization work is taken in that
  order — don't skip ahead on a hunch.

## Known gaps to account for before trusting the numbers

- `agave_shred_fetch_queue_length` is not wired yet — will read empty.
- The `retransmit` label assumes "refetch" in the original ask meant "retransmit
  to other validators" — not yet confirmed with the user.
- No production run has happened yet, so there's no prior baseline to compare
  against on this branch (the `custom-pipeline` branch's InfluxDB-based numbers
  are a different instrumentation methodology and shouldn't be treated as
  equivalent).

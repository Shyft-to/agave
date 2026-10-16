//! A Prometheus registry and metric-registration helpers, separate from the
//! `datapoint!`/InfluxDB pipeline in [`crate::metrics`].
//!
//! Metrics registered here are served over an HTTP `/metrics` endpoint (see
//! [`crate::prometheus_server`]) rather than pushed to InfluxDB.

use {
    prometheus::{
        Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, IntCounter,
        IntCounterVec, Opts, Registry, TextEncoder,
    },
    std::sync::LazyLock,
};

/// A dedicated registry so nothing else in the process can register into it by
/// accident (as could happen with `prometheus::default_registry()`).
pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

/// Bucket boundaries, in microseconds, spanning ~10us to 5s. All duration
/// metrics in this module are in microseconds, not Prometheus's usual
/// base-unit-in-seconds convention.
///
/// The top end (1s/2s/5s) exists specifically for `agave_end_to_end_duration_us`
/// (path=executed_tx can legitimately span multiple slots on a real cluster --
/// skipped slots, big blocks, a validator briefly falling behind). Without these,
/// `histogram_quantile` silently clips any p99/p999 above the highest finite
/// bucket to that bucket's boundary instead of showing the true value -- this
/// was observed in practice (a reported p99 of exactly 500000us, the old
/// ceiling, on 2026-09-29's baseline run) before this range was widened.
pub const DURATION_US_BUCKETS: &[f64] = &[
    10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1_000.0, 2_000.0, 5_000.0, 10_000.0, 20_000.0,
    50_000.0, 100_000.0, 500_000.0, 1_000_000.0, 2_000_000.0, 5_000_000.0,
];

pub fn register_histogram(name: &str, help: &str) -> Histogram {
    let opts = HistogramOpts::new(name, help).buckets(DURATION_US_BUCKETS.to_vec());
    let metric = Histogram::with_opts(opts).expect("valid histogram metric");
    REGISTRY
        .register(Box::new(metric.clone()))
        .expect("metric name not already registered");
    metric
}

pub fn register_histogram_vec(name: &str, help: &str, label_names: &[&str]) -> HistogramVec {
    let opts = HistogramOpts::new(name, help).buckets(DURATION_US_BUCKETS.to_vec());
    let metric = HistogramVec::new(opts, label_names).expect("valid histogram metric");
    REGISTRY
        .register(Box::new(metric.clone()))
        .expect("metric name not already registered");
    metric
}

pub fn register_int_counter(name: &str, help: &str) -> IntCounter {
    let metric = IntCounter::new(name, help).expect("valid counter metric");
    REGISTRY
        .register(Box::new(metric.clone()))
        .expect("metric name not already registered");
    metric
}

pub fn register_int_counter_vec(name: &str, help: &str, label_names: &[&str]) -> IntCounterVec {
    let opts = Opts::new(name, help);
    let metric = IntCounterVec::new(opts, label_names).expect("valid counter metric");
    REGISTRY
        .register(Box::new(metric.clone()))
        .expect("metric name not already registered");
    metric
}

pub fn register_gauge(name: &str, help: &str) -> Gauge {
    let metric = Gauge::new(name, help).expect("valid gauge metric");
    REGISTRY
        .register(Box::new(metric.clone()))
        .expect("metric name not already registered");
    metric
}

pub fn register_gauge_vec(name: &str, help: &str, label_names: &[&str]) -> GaugeVec {
    let opts = Opts::new(name, help);
    let metric = GaugeVec::new(opts, label_names).expect("valid gauge metric");
    REGISTRY
        .register(Box::new(metric.clone()))
        .expect("metric name not already registered");
    metric
}

/// Renders every metric currently in [`REGISTRY`] as Prometheus text-exposition
/// format, for the `/metrics` HTTP handler to serve directly.
pub fn gather_text() -> String {
    let metric_families = REGISTRY.gather();
    let encoder = TextEncoder::new();
    let mut buffer = Vec::new();
    encoder
        .encode(&metric_families, &mut buffer)
        .expect("encoding a gathered registry never fails");
    String::from_utf8(buffer).expect("prometheus text encoder always produces valid utf8")
}

#[cfg(test)]
mod test {
    use {super::*, std::sync::LazyLock};

    #[test]
    fn test_register_and_gather_counter() {
        static COUNTER: LazyLock<IntCounter> =
            LazyLock::new(|| register_int_counter("test_prom_counter_total", "a test counter"));
        COUNTER.inc_by(7);

        let text = gather_text();
        assert!(text.contains("test_prom_counter_total 7"));
    }

    #[test]
    fn test_register_and_gather_histogram_vec() {
        static HISTOGRAM: LazyLock<HistogramVec> = LazyLock::new(|| {
            register_histogram_vec(
                "test_prom_stage_duration_us",
                "a test histogram",
                &["stage"],
            )
        });
        HISTOGRAM.with_label_values(&["fetch"]).observe(123.0);

        let text = gather_text();
        assert!(text.contains("test_prom_stage_duration_us_bucket"));
        assert!(text.contains("stage=\"fetch\""));
        assert!(text.contains("test_prom_stage_duration_us_sum{stage=\"fetch\"} 123"));
    }

    #[test]
    fn test_register_and_gather_gauge() {
        static GAUGE: LazyLock<Gauge> =
            LazyLock::new(|| register_gauge("test_prom_gauge", "a test gauge"));
        GAUGE.set(42.0);

        let text = gather_text();
        assert!(text.contains("test_prom_gauge 42"));
    }
}

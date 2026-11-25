//! Prometheus RED metrics registry: hand-rolled counters and histograms
//! rendered in the text exposition format (version 0.0.4).
//!
//! Push families are incremented on the hot paths (inbound Data Plane
//! requests, per-Item rate limiting, outbound `Upstream` delivery);
//! scrape-time samples (queue utilization, `Upstream Health`, reload
//! status) are rendered by the Admin Plane next to [`Metrics::render`].
//!
//! Inbound requests are counted at the Data Plane port: every request the
//! listener accepts — `Ingestion Route` and `Pass-Through Traffic` alike —
//! lands in `http_requests_total{direction="inbound",...}` exactly once.
//! What the service sends onwards to the `Upstream` is counted separately
//! in the `upstream_*` families, never mixed into the inbound counter.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// The fallback `environment` label value: every environment outside the
/// configured allowlist (including missing ones) maps here, bounding the
/// Prometheus cardinality.
pub const OTHER_ENVIRONMENT: &str = "other";

/// Upper bounds of the latency histogram buckets, in seconds. The implicit
/// `+Inf` bucket is rendered automatically.
const LATENCY_BUCKET_BOUNDS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Normalizes an `environment` metric label against the allowlist: listed
/// values pass through verbatim, everything else (including the empty
/// string and the codec's `"unknown"` placeholder) maps to `"other"`.
pub fn normalize_environment<'a>(allowlist: &[String], environment: &'a str) -> &'a str {
    if !environment.is_empty() && allowlist.iter().any(|allowed| allowed == environment) {
        environment
    } else {
        OTHER_ENVIRONMENT
    }
}

/// Escapes a label value per the Prometheus text exposition format.
fn escape_label_value(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            other => escaped.push(other),
        }
    }
    escaped
}

/// Renders `name{label="value",...}` with escaped label values; a bare
/// `name` when there are no labels.
fn render_metric_name(name: &str, labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return name.to_string();
    }
    let rendered: Vec<String> = labels
        .iter()
        .map(|(label, value)| format!("{label}=\"{}\"", escape_label_value(value)))
        .collect();
    format!("{name}{{{}}}", rendered.join(","))
}

/// A stable key identifying one series by its label set.
fn series_key(labels: &[(String, String)]) -> String {
    labels
        .iter()
        .map(|(label, value)| format!("{label}\u{1}{value}"))
        .collect::<Vec<_>>()
        .join("\u{2}")
}

/// Locks a family series map, tolerating lock poisoning: metric recording
/// must never take the data path down with it.
fn lock_series<T>(series: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    series
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One counter series.
struct CounterSeries {
    labels: Vec<(String, String)>,
    value: AtomicU64,
}

/// A counter family: series are created lazily on first use.
struct CounterFamily {
    name: &'static str,
    help: &'static str,
    series: Mutex<HashMap<String, Arc<CounterSeries>>>,
}

impl CounterFamily {
    fn new(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            series: Mutex::new(HashMap::new()),
        }
    }

    /// Increments the series identified by `labels` by `amount`, creating
    /// it on first use.
    fn increment_by(&self, labels: Vec<(String, String)>, amount: u64) {
        let key = series_key(&labels);
        let mut series = lock_series(&self.series);
        if let Some(existing) = series.get(&key) {
            existing.value.fetch_add(amount, Ordering::Relaxed);
            return;
        }
        series.insert(
            key,
            Arc::new(CounterSeries {
                labels,
                value: AtomicU64::new(amount),
            }),
        );
    }

    /// Increments the series identified by `labels`, creating it on first
    /// use.
    fn increment(&self, labels: Vec<(String, String)>) {
        self.increment_by(labels, 1);
    }

    /// Appends the family (HELP/TYPE headers plus every series) to `out`.
    fn render(&self, out: &mut String) {
        out.push_str(&format!(
            "# HELP {} {}\n# TYPE {} counter\n",
            self.name, self.help, self.name
        ));
        let series = lock_series(&self.series);
        let mut ordered: Vec<&Arc<CounterSeries>> = series.values().collect();
        ordered.sort_by_key(|sample| series_key(&sample.labels));
        for sample in ordered {
            out.push_str(&format!(
                "{} {}\n",
                render_metric_name(self.name, &sample.labels),
                sample.value.load(Ordering::Relaxed)
            ));
        }
    }
}

/// One histogram series: non-cumulative per-bucket counts plus the total
/// count and the nanosecond-accurate sum.
struct HistogramSeries {
    labels: Vec<(String, String)>,
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    sum_nanos: AtomicU64,
}

/// A histogram family: series are created lazily on first use.
struct HistogramFamily {
    name: &'static str,
    help: &'static str,
    series: Mutex<HashMap<String, Arc<HistogramSeries>>>,
}

impl HistogramFamily {
    fn new(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            series: Mutex::new(HashMap::new()),
        }
    }

    /// Records one observation into the series identified by `labels`.
    fn observe(&self, labels: Vec<(String, String)>, duration: Duration) {
        let seconds = duration.as_secs_f64();
        let key = series_key(&labels);
        let mut series = lock_series(&self.series);
        let sample = match series.get(&key) {
            Some(existing) => Arc::clone(existing),
            None => {
                let created = Arc::new(HistogramSeries {
                    labels,
                    buckets: LATENCY_BUCKET_BOUNDS
                        .iter()
                        .map(|_| AtomicU64::new(0))
                        .collect(),
                    count: AtomicU64::new(0),
                    sum_nanos: AtomicU64::new(0),
                });
                series.insert(key, Arc::clone(&created));
                created
            }
        };
        drop(series);

        if let Some(bucket) = LATENCY_BUCKET_BOUNDS
            .iter()
            .position(|bound| seconds <= *bound)
        {
            sample.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        }
        sample.count.fetch_add(1, Ordering::Relaxed);
        sample
            .sum_nanos
            .fetch_add(duration.as_nanos() as u64, Ordering::Relaxed);
    }

    /// Appends the family to `out`: cumulative `le` buckets, `+Inf`, `_sum`
    /// and `_count` per series.
    fn render(&self, out: &mut String) {
        out.push_str(&format!(
            "# HELP {} {}\n# TYPE {} histogram\n",
            self.name, self.help, self.name
        ));
        let series = lock_series(&self.series);
        let mut ordered: Vec<&Arc<HistogramSeries>> = series.values().collect();
        ordered.sort_by_key(|sample| series_key(&sample.labels));
        for sample in ordered {
            let mut cumulative: u64 = 0;
            for (index, bound) in LATENCY_BUCKET_BOUNDS.iter().enumerate() {
                cumulative += sample.buckets[index].load(Ordering::Relaxed);
                let mut labels = sample.labels.clone();
                labels.push(("le".to_string(), format!("{bound}")));
                out.push_str(&format!(
                    "{} {}\n",
                    render_metric_name(self.name, &labels),
                    cumulative
                ));
            }
            let mut labels = sample.labels.clone();
            labels.push(("le".to_string(), "+Inf".to_string()));
            out.push_str(&format!(
                "{} {}\n",
                render_metric_name(self.name, &labels),
                sample.count.load(Ordering::Relaxed)
            ));
            let sum = sample.sum_nanos.load(Ordering::Relaxed) as f64 / 1e9;
            let labels = render_labels_suffix(&sample.labels);
            out.push_str(&format!(
                "{}_sum{} {}\n{}_count{} {}\n",
                self.name,
                labels,
                sum,
                self.name,
                labels,
                sample.count.load(Ordering::Relaxed)
            ));
        }
    }
}

/// Renders just the `{label="value",...}` suffix of a label set.
fn render_labels_suffix(labels: &[(String, String)]) -> String {
    if labels.is_empty() {
        return String::new();
    }
    let rendered: Vec<String> = labels
        .iter()
        .map(|(label, value)| format!("{label}=\"{}\"", escape_label_value(value)))
        .collect();
    format!("{{{}}}", rendered.join(","))
}

/// The RED metrics registry shared by the Data Plane, the ingestion
/// pipeline and the delivery pool.
pub struct Metrics {
    http_requests_total: CounterFamily,
    http_request_duration_seconds: HistogramFamily,
    ingest_items_total: CounterFamily,
    upstream_requests_total: CounterFamily,
    upstream_request_duration_seconds: HistogramFamily,
    upstream_batch_items: CounterFamily,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    /// Builds an empty registry.
    pub fn new() -> Self {
        Self {
            http_requests_total: CounterFamily::new(
                "http_requests_total",
                "Requests received on the Data Plane port, by direction, route kind, project, environment and outcome.",
            ),
            http_request_duration_seconds: HistogramFamily::new(
                "http_request_duration_seconds",
                "Latency of requests handled on the Data Plane port.",
            ),
            ingest_items_total: CounterFamily::new(
                "ingest_items_total",
                "Envelope and store Items evaluated against Rate Limit Rules, by project, environment, category and action.",
            ),
            upstream_requests_total: CounterFamily::new(
                "upstream_requests_total",
                "Delivery attempts sent to the Upstream, by outcome.",
            ),
            upstream_request_duration_seconds: HistogramFamily::new(
                "upstream_request_duration_seconds",
                "Latency of delivery attempts sent to the Upstream.",
            ),
            upstream_batch_items: CounterFamily::new(
                "upstream_batch_items",
                "Queued records carried by completed outbound batches, by outcome.",
            ),
        }
    }

    /// Records one inbound Data Plane request: the port-level RED pair of
    /// `http_requests_total` and `http_request_duration_seconds`.
    pub fn record_http_request(
        &self,
        route_kind: &str,
        project_id: &str,
        environment: &str,
        outcome: &str,
        duration: Duration,
    ) {
        let counter_labels = vec![
            ("direction".to_string(), "inbound".to_string()),
            ("route_kind".to_string(), route_kind.to_string()),
            ("project_id".to_string(), project_id.to_string()),
            ("environment".to_string(), environment.to_string()),
            ("outcome".to_string(), outcome.to_string()),
        ];
        self.http_requests_total.increment(counter_labels);
        let histogram_labels = vec![
            ("route_kind".to_string(), route_kind.to_string()),
            ("outcome".to_string(), outcome.to_string()),
        ];
        self.http_request_duration_seconds
            .observe(histogram_labels, duration);
    }

    /// Records the rate-limit outcome of one envelope or store Item.
    pub fn record_ingest_item(
        &self,
        project_id: &str,
        environment: &str,
        category: &str,
        action: &str,
    ) {
        self.ingest_items_total.increment(vec![
            ("project_id".to_string(), project_id.to_string()),
            ("environment".to_string(), environment.to_string()),
            ("category".to_string(), category.to_string()),
            ("action".to_string(), action.to_string()),
        ]);
    }

    /// Records one outbound delivery attempt to the `Upstream`.
    pub fn record_upstream_attempt(&self, outcome: &str, duration: Duration) {
        self.upstream_requests_total
            .increment(vec![("outcome".to_string(), outcome.to_string())]);
        self.upstream_request_duration_seconds
            .observe(vec![("outcome".to_string(), outcome.to_string())], duration);
    }

    /// Records the queued records carried by one completed outbound batch.
    pub fn record_upstream_batch(&self, outcome: &str, items: usize) {
        self.upstream_batch_items.increment_by(
            vec![("outcome".to_string(), outcome.to_string())],
            items as u64,
        );
    }

    /// Renders every push family in the Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut out = String::new();
        self.http_requests_total.render(&mut out);
        self.http_request_duration_seconds.render(&mut out);
        self.ingest_items_total.render(&mut out);
        self.upstream_requests_total.render(&mut out);
        self.upstream_request_duration_seconds.render(&mut out);
        self.upstream_batch_items.render(&mut out);
        out
    }
}

/// One scrape-time sample (gauge or counter) rendered by the Admin Plane
/// next to the push families.
pub struct ScrapeSample {
    pub name: &'static str,
    pub help: &'static str,
    /// `gauge` or `counter`.
    pub kind: &'static str,
    pub labels: Vec<(&'static str, String)>,
    pub value: f64,
}

/// Renders scrape-time samples: one HELP/TYPE header pair per distinct
/// metric name, then every sample line of that family.
pub fn render_scrape_samples(samples: &[ScrapeSample]) -> String {
    let mut out = String::new();
    let mut names: Vec<&str> = Vec::new();
    for sample in samples {
        if !names.contains(&sample.name) {
            names.push(sample.name);
        }
    }
    for name in names {
        let family = samples
            .iter()
            .find(|sample| sample.name == name)
            .expect("every name came from the samples");
        out.push_str(&format!(
            "# HELP {name} {}\n# TYPE {name} {}\n",
            family.help, family.kind
        ));
        for sample in samples.iter().filter(|sample| sample.name == name) {
            let labels: Vec<(String, String)> = sample
                .labels
                .iter()
                .map(|(label, value)| (label.to_string(), value.clone()))
                .collect();
            out.push_str(&format!(
                "{} {}\n",
                render_metric_name(sample.name, &labels),
                sample.value
            ));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allowlist(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn normalizes_environments_against_the_allowlist() {
        let allowlist = allowlist(&["stage", "prod"]);

        assert_eq!(normalize_environment(&allowlist, "prod"), "prod");
        assert_eq!(normalize_environment(&allowlist, "stage"), "stage");
        assert_eq!(normalize_environment(&allowlist, "dev"), OTHER_ENVIRONMENT);
        assert_eq!(normalize_environment(&allowlist, ""), OTHER_ENVIRONMENT);
        assert_eq!(
            normalize_environment(&allowlist, "unknown"),
            OTHER_ENVIRONMENT,
            "the codec's unknown placeholder maps to other"
        );
    }

    #[test]
    fn renders_counters_in_the_text_exposition_format() {
        let metrics = Metrics::new();
        metrics.record_http_request("ingest", "42", "prod", "accepted", Duration::from_millis(5));
        metrics.record_http_request("ingest", "42", "prod", "accepted", Duration::from_millis(5));
        metrics.record_http_request(
            "ingest",
            "42",
            "prod",
            "throttled",
            Duration::from_millis(5),
        );

        let text = metrics.render();

        assert!(text.contains("# TYPE http_requests_total counter"));
        assert!(
            text.contains(
                "http_requests_total{direction=\"inbound\",route_kind=\"ingest\",project_id=\"42\",environment=\"prod\",outcome=\"accepted\"} 2"
            ),
            "every inbound request is counted at the port, marked with the inbound direction:\n{text}"
        );
        assert!(
            text.contains(
                "http_requests_total{direction=\"inbound\",route_kind=\"ingest\",project_id=\"42\",environment=\"prod\",outcome=\"throttled\"} 1"
            ),
            "{text}"
        );
    }

    #[test]
    fn renders_the_request_latency_histogram() {
        let metrics = Metrics::new();
        metrics.record_http_request(
            "ingest",
            "42",
            "prod",
            "accepted",
            Duration::from_millis(600),
        );
        metrics.record_http_request(
            "ingest",
            "42",
            "prod",
            "accepted",
            Duration::from_millis(40),
        );

        let text = metrics.render();

        assert!(text.contains("# TYPE http_request_duration_seconds histogram"));
        assert!(
            text.contains(
                "http_request_duration_seconds{route_kind=\"ingest\",outcome=\"accepted\",le=\"0.5\"} 1"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "http_request_duration_seconds{route_kind=\"ingest\",outcome=\"accepted\",le=\"1\"} 2"
            ),
            "buckets are cumulative",
        );
        assert!(
            text.contains(
                "http_request_duration_seconds{route_kind=\"ingest\",outcome=\"accepted\",le=\"+Inf\"} 2"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "http_request_duration_seconds_sum{route_kind=\"ingest\",outcome=\"accepted\"} 0.64"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "http_request_duration_seconds_count{route_kind=\"ingest\",outcome=\"accepted\"} 2"
            ),
            "{text}"
        );
    }

    #[test]
    fn renders_ingest_item_and_upstream_families() {
        let metrics = Metrics::new();
        metrics.record_ingest_item("42", "prod", "event", "accepted");
        metrics.record_ingest_item("42", "other", "transaction", "throttled");
        metrics.record_upstream_attempt("success", Duration::from_millis(10));
        metrics.record_upstream_batch("success", 3);

        let text = metrics.render();

        assert!(
            text.contains(
                "ingest_items_total{project_id=\"42\",environment=\"prod\",category=\"event\",action=\"accepted\"} 1"
            ),
            "{text}"
        );
        assert!(
            text.contains(
                "ingest_items_total{project_id=\"42\",environment=\"other\",category=\"transaction\",action=\"throttled\"} 1"
            ),
            "{text}"
        );
        assert!(text.contains("upstream_requests_total{outcome=\"success\"} 1"));
        assert!(text.contains("upstream_batch_items{outcome=\"success\"} 3"));
        assert!(text.contains("# TYPE upstream_request_duration_seconds histogram"));
    }

    #[test]
    fn escapes_label_values() {
        assert_eq!(
            escape_label_value("a\"b\\c\nd"),
            "a\\\"b\\\\c\\nd",
            "quotes, backslashes and newlines must be escaped"
        );
    }

    #[test]
    fn renders_scrape_time_samples() {
        let samples = vec![
            ScrapeSample {
                name: "upstream_healthy",
                help: "1 while Upstream Health is Healthy.",
                kind: "gauge",
                labels: vec![],
                value: 1.0,
            },
            ScrapeSample {
                name: "queue_dropped_total",
                help: "Records dropped from the Ingestion Queue, by reason.",
                kind: "counter",
                labels: vec![("reason", "expired".to_string())],
                value: 2.0,
            },
            ScrapeSample {
                name: "queue_dropped_total",
                help: "Records dropped from the Ingestion Queue, by reason.",
                kind: "counter",
                labels: vec![("reason", "overflow".to_string())],
                value: 0.0,
            },
        ];

        let text = render_scrape_samples(&samples);

        assert!(text.contains("# TYPE upstream_healthy gauge"));
        assert!(text.contains("upstream_healthy 1"));
        assert!(text.contains("# TYPE queue_dropped_total counter"));
        assert!(text.contains("queue_dropped_total{reason=\"expired\"} 2"));
        assert!(text.contains("queue_dropped_total{reason=\"overflow\"} 0"));
    }
}

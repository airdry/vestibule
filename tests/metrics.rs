//! End-to-end tests for Prometheus RED observability: the Admin Plane
//! `/metrics` exposition covering inbound Data Plane traffic, item rate
//! limiting, outbound delivery, queue saturation and operational gauges.

mod support;

use support::{MockUpstream, UpstreamResponse, http_client, metric_value, scrape_metrics};

/// Starts the service with extra YAML sections appended after `queue`.
fn start_with(extra_sections: &str) -> (support::TestApp, MockUpstream) {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    let config = support::config_yaml_with(
        &upstream.url(),
        support::ephemeral_port(),
        support::ephemeral_port(),
        extra_sections,
    );
    (support::start_service(&config), upstream)
}

/// Builds a config with extra `upstream.healthcheck` keys, extra `queue`
/// keys (indented under the `queue` section) plus extra sections appended
/// after it.
fn queue_config(
    upstream_url: &str,
    healthcheck_keys: &str,
    queue_keys: &str,
    extra_sections: &str,
) -> String {
    format!(
        "server:\n\
         \x20 data_plane_addr: \"127.0.0.1:{}\"\n\
         \x20 admin_addr: \"127.0.0.1:{}\"\n\
         upstream:\n\
         \x20 url: \"{upstream_url}\"\n\
         \x20 healthcheck:\n\
         \x20 \x20 path: \"{}\"\n\
         {healthcheck_keys}queue:\n\
         \x20 dir: \"{}\"\n\
         {queue_keys}{extra_sections}",
        support::ephemeral_port(),
        support::ephemeral_port(),
        support::PROBE_PATH,
        support::tempdir().display(),
    )
}

fn start() -> (support::TestApp, MockUpstream) {
    start_with("")
}

/// POSTs an ingestion body to the Data Plane.
fn post_ingest(app: &support::TestApp, path: &str, body: Vec<u8>) -> reqwest::blocking::Response {
    http_client()
        .post(format!("{}{path}", app.data_plane_url))
        .header("Content-Type", "application/x-sentry-envelope")
        .body(body)
        .send()
        .expect("POST ingestion request")
}

/// A single-item envelope in the given environment.
fn envelope(environment: &str) -> Vec<u8> {
    let item_body = format!("{{\"environment\":\"{environment}\",\"m\":1}}");
    let mut payload = b"{\"event_id\":\"84df5a5f-51d8-409a-9416-a1adb4b0e3d0\"}\n".to_vec();
    payload.extend_from_slice(
        format!("{{\"type\":\"event\",\"length\":{}}}\n", item_body.len()).as_bytes(),
    );
    payload.extend_from_slice(item_body.as_bytes());
    payload.push(b'\n');
    payload
}

#[test]
fn metrics_endpoint_serves_prometheus_text_with_gauges() {
    let (app, _upstream) = start();

    let response = http_client()
        .get(format!("{}/metrics", app.admin_url))
        .send()
        .expect("GET /metrics on the Admin Plane");

    assert_eq!(response.status().as_u16(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/plain; version=0.0.4; charset=utf-8")
    );
    let body = response.text().expect("read the exposition body");

    // Push families with their HELP/TYPE headers.
    for header in [
        "# TYPE http_requests_total counter",
        "# TYPE http_request_duration_seconds histogram",
        "# TYPE ingest_items_total counter",
        "# TYPE upstream_requests_total counter",
        "# TYPE upstream_request_duration_seconds histogram",
        "# TYPE upstream_batch_items counter",
    ] {
        assert!(body.contains(header), "missing {header} in:\n{body}");
    }
    // Scrape-time gauges and counters.
    for header in [
        "# TYPE queue_bytes gauge",
        "# TYPE queue_segments gauge",
        "# TYPE queue_items gauge",
        "# TYPE queue_oldest_age_seconds gauge",
        "# TYPE queue_dropped_total counter",
        "# TYPE upstream_healthy gauge",
        "# TYPE config_reload_success gauge",
    ] {
        assert!(body.contains(header), "missing {header} in:\n{body}");
    }
}

#[test]
fn counts_inbound_requests_at_the_port_by_direction_and_outcome() {
    let (app, upstream) = start();

    // One accepted ingestion request and one pass-through request: both
    // hit the Data Plane port, so both are counted in http_requests_total.
    let response = post_ingest(&app, "/api/42/envelope/", envelope("prod"));
    assert_eq!(response.status().as_u16(), 201);
    let passthrough = http_client()
        .get(format!("{}/some/ui/path", app.data_plane_url))
        .send()
        .expect("GET pass-through request");
    assert_eq!(passthrough.status().as_u16(), 200);
    assert_eq!(passthrough.text().expect("body"), "upstream body");

    let samples = scrape_metrics(&app);

    assert_eq!(
        metric_value(
            &samples,
            "http_requests_total",
            &[
                ("direction", "inbound"),
                ("route_kind", "ingest"),
                ("project_id", "42"),
                ("environment", "prod"),
                ("outcome", "accepted"),
            ]
        ),
        Some(1.0),
        "the SDK-sent request is counted at the port and marked as inbound"
    );
    assert_eq!(
        metric_value(
            &samples,
            "http_requests_total",
            &[
                ("direction", "inbound"),
                ("route_kind", "passthrough"),
                ("project_id", ""),
                ("environment", "other"),
                ("outcome", "forwarded"),
            ]
        ),
        Some(1.0),
        "pass-through traffic is counted at the port too"
    );
    assert_eq!(
        metric_value(
            &samples,
            "http_request_duration_seconds_count",
            &[("route_kind", "ingest"), ("outcome", "accepted")],
        ),
        Some(1.0),
        "the mandatory request latency histogram observed the request"
    );
    assert_eq!(
        metric_value(
            &samples,
            "http_request_duration_seconds_count",
            &[("route_kind", "passthrough"), ("outcome", "forwarded")],
        ),
        Some(1.0)
    );

    // What the service sent onwards to the Upstream is counted separately,
    // never inside the inbound counter.
    support::wait_until(|| {
        metric_value(
            &scrape_metrics(&app),
            "upstream_requests_total",
            &[("outcome", "success")],
        ) == Some(1.0)
    });
    let _ = upstream;
}

#[test]
fn counts_throttled_requests_with_the_throttled_outcome() {
    let (app, _upstream) = start_with(
        "rate_limits:\n  max_buckets: 100\n  defaults:\n    event: {rate: 1, burst: 1}\n",
    );

    // First request passes, second is fully throttled.
    assert_eq!(
        post_ingest(&app, "/api/42/envelope/", envelope("prod"))
            .status()
            .as_u16(),
        201
    );
    assert_eq!(
        post_ingest(&app, "/api/42/envelope/", envelope("prod"))
            .status()
            .as_u16(),
        429
    );

    let samples = scrape_metrics(&app);
    assert_eq!(
        metric_value(
            &samples,
            "http_requests_total",
            &[
                ("direction", "inbound"),
                ("route_kind", "ingest"),
                ("project_id", "42"),
                ("environment", "prod"),
                ("outcome", "accepted"),
            ]
        ),
        Some(1.0)
    );
    assert_eq!(
        metric_value(
            &samples,
            "http_requests_total",
            &[
                ("direction", "inbound"),
                ("route_kind", "ingest"),
                ("project_id", "42"),
                ("environment", "prod"),
                ("outcome", "throttled"),
            ]
        ),
        Some(1.0)
    );
}

#[test]
fn buckets_ingest_items_by_environment_allowlist() {
    let (app, _upstream) = start_with(
        "observability:\n  environment_label_allowlist: [dev]\nrate_limits:\n  max_buckets: 100\n",
    );

    // One envelope with a dev item and a not-allowed production item.
    let mut payload = b"{}\n".to_vec();
    let dev_body = br#"{"environment":"dev","m":1}"#;
    let prod_body = br#"{"environment":"production","m":1}"#;
    payload.extend_from_slice(
        format!("{{\"type\":\"event\",\"length\":{}}}\n", dev_body.len()).as_bytes(),
    );
    payload.extend_from_slice(dev_body);
    payload.push(b'\n');
    payload.extend_from_slice(
        format!("{{\"type\":\"event\",\"length\":{}}}\n", prod_body.len()).as_bytes(),
    );
    payload.extend_from_slice(prod_body);
    payload.push(b'\n');

    let response = post_ingest(&app, "/api/42/envelope/", payload);
    assert_eq!(response.status().as_u16(), 201);

    let samples = scrape_metrics(&app);
    assert_eq!(
        metric_value(
            &samples,
            "ingest_items_total",
            &[
                ("project_id", "42"),
                ("environment", "dev"),
                ("category", "event"),
                ("action", "accepted"),
            ]
        ),
        Some(1.0),
        "allowlisted environments keep their own label"
    );
    assert_eq!(
        metric_value(
            &samples,
            "ingest_items_total",
            &[
                ("project_id", "42"),
                ("environment", "other"),
                ("category", "event"),
                ("action", "accepted"),
            ]
        ),
        Some(1.0),
        "unlisted environments map to other"
    );
    // The request-level environment label follows the first item's
    // environment, normalized by the same allowlist.
    assert_eq!(
        metric_value(
            &samples,
            "http_requests_total",
            &[
                ("direction", "inbound"),
                ("route_kind", "ingest"),
                ("project_id", "42"),
                ("environment", "dev"),
                ("outcome", "accepted"),
            ]
        ),
        Some(1.0)
    );
}

#[test]
fn counts_upstream_delivery_and_batch_items() {
    let (app, _upstream) = start_with("batch:\n  flush_interval: 50ms\n");

    assert_eq!(
        post_ingest(&app, "/api/42/envelope/", envelope("prod"))
            .status()
            .as_u16(),
        201
    );
    // The delivery metrics are recorded after the response arrives, so wait
    // on the metric itself instead of on the recorded request.
    support::wait_until(|| {
        metric_value(
            &scrape_metrics(&app),
            "upstream_requests_total",
            &[("outcome", "success")],
        ) == Some(1.0)
    });

    let samples = scrape_metrics(&app);
    assert_eq!(
        metric_value(
            &samples,
            "upstream_requests_total",
            &[("outcome", "success")]
        ),
        Some(1.0)
    );
    assert_eq!(
        metric_value(&samples, "upstream_batch_items", &[("outcome", "success")]),
        Some(1.0)
    );
    assert_eq!(
        metric_value(
            &samples,
            "upstream_request_duration_seconds_count",
            &[("outcome", "success")],
        ),
        Some(1.0)
    );
}

#[test]
fn reports_queue_saturation_gauges_while_batches_wait() {
    // A long flush interval keeps the record in the Ingestion Queue, so
    // the queue gauges are deterministically non-zero.
    let (app, _upstream) = start_with("batch:\n  flush_interval: 60s\n");

    assert_eq!(
        post_ingest(&app, "/api/42/envelope/", envelope("prod"))
            .status()
            .as_u16(),
        201
    );

    support::wait_until(|| {
        let samples = scrape_metrics(&app);
        metric_value(&samples, "queue_items", &[]) >= Some(1.0)
            && metric_value(&samples, "queue_bytes", &[]) >= Some(1.0)
    });

    let samples = scrape_metrics(&app);
    assert!(metric_value(&samples, "queue_segments", &[]).unwrap_or(0.0) >= 1.0);
    assert!(metric_value(&samples, "queue_oldest_age_seconds", &[]).unwrap_or(0.0) >= 0.0);
}

#[test]
fn counts_ttl_expired_drops() {
    // The Upstream fails, so Upstream Health turns Unhealthy and the
    // dispatcher buffers the record instead of claiming it. Waiting for the
    // record to age past `max_age` on the queue gauge guarantees it is
    // expired when the health transition claims it, instead of depending on
    // whether the dispatcher's first tick runs before or after the ingest.
    let upstream = MockUpstream::start(UpstreamResponse::with_status(503));
    let config = queue_config(
        &upstream.url(),
        "    interval: 50ms\n    timeout: 1s\n    unhealthy_threshold: 1\n    healthy_threshold: 1\n",
        "  max_age: 1s\n",
        "batch:\n  flush_interval: 2s\n",
    );
    let app = support::start_service(&config);

    support::wait_until_health(&app, false);

    assert_eq!(
        post_ingest(&app, "/api/42/envelope/", envelope("prod"))
            .status()
            .as_u16(),
        201
    );

    // `max_age` is 1s and the TTL check is strict, so wait for a margin.
    support::wait_until(|| {
        metric_value(&scrape_metrics(&app), "queue_oldest_age_seconds", &[]) >= Some(1.1)
    });

    upstream.set_response(UpstreamResponse::ok("upstream body"));

    support::wait_until(|| {
        metric_value(
            &scrape_metrics(&app),
            "queue_dropped_total",
            &[("reason", "expired")],
        ) >= Some(1.0)
    });
}

#[test]
fn counts_overflow_evicted_drops() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    // Every record lands in its own segment (segment_size_bytes: 1) and the
    // 1-byte max_bytes evicts the oldest closed segment on the second
    // append.
    let config = queue_config(
        &upstream.url(),
        "",
        "  max_bytes: 1\n  segment_size_bytes: 1\n",
        "batch:\n  flush_interval: 60s\n",
    );
    let app = support::start_service(&config);

    assert_eq!(
        post_ingest(&app, "/api/42/envelope/", envelope("prod"))
            .status()
            .as_u16(),
        201
    );
    assert_eq!(
        post_ingest(&app, "/api/42/envelope/", envelope("prod"))
            .status()
            .as_u16(),
        201
    );

    support::wait_until(|| {
        metric_value(
            &scrape_metrics(&app),
            "queue_dropped_total",
            &[("reason", "overflow")],
        ) >= Some(1.0)
    });
}

#[test]
fn upstream_healthy_gauge_tracks_the_health_state() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    let config = format!(
        "server:\n\
         \x20 data_plane_addr: \"127.0.0.1:{}\"\n\
         \x20 admin_addr: \"127.0.0.1:{}\"\n\
         upstream:\n\
         \x20 url: \"{}\"\n\
         \x20 healthcheck:\n\
         \x20 \x20 path: \"{}\"\n\
         \x20 \x20 interval: 50ms\n\
         \x20 \x20 timeout: 1s\n\
         \x20 \x20 unhealthy_threshold: 1\n\
         \x20 \x20 healthy_threshold: 1\n\
         queue:\n\
         \x20 dir: \"{}\"\n",
        support::ephemeral_port(),
        support::ephemeral_port(),
        upstream.url(),
        support::PROBE_PATH,
        support::tempdir().display(),
    );
    let app = support::start_service(&config);

    let samples = scrape_metrics(&app);
    assert_eq!(metric_value(&samples, "upstream_healthy", &[]), Some(1.0));

    upstream.set_response(UpstreamResponse::with_status(503));
    support::wait_until(|| {
        metric_value(&scrape_metrics(&app), "upstream_healthy", &[]) == Some(0.0)
    });

    upstream.set_response(UpstreamResponse::ok("recovered"));
    support::wait_until(|| {
        metric_value(&scrape_metrics(&app), "upstream_healthy", &[]) == Some(1.0)
    });
}

/// Starts the service from a config file and returns it with the config
/// path (kept for rewriting the file during reload tests).
fn start_with_config(
    upstream_url: &str,
    extra_sections: &str,
) -> (support::TestApp, std::path::PathBuf) {
    let yaml = support::config_yaml_for(
        upstream_url,
        support::ephemeral_port(),
        support::ephemeral_port(),
        &support::tempdir(),
        extra_sections,
    );
    let path = support::write_config(&yaml);
    let app = support::start_service_from_config(&path);
    (app, path)
}

#[test]
fn config_reload_success_gauge_tracks_reload_outcomes() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("upstream body"));
    let (app, config_path) = start_with_config(&upstream.url(), "");

    let samples = scrape_metrics(&app);
    assert_eq!(
        metric_value(&samples, "config_reload_success", &[]),
        Some(1.0)
    );

    // A rejected reload flips the gauge to 0.
    std::fs::write(&config_path, "not: [valid, yaml").expect("break the config file");
    let response = http_client()
        .post(format!("{}/reload", app.admin_url))
        .send()
        .expect("POST /reload");
    assert_eq!(response.status().as_u16(), 400);
    support::wait_until(|| {
        metric_value(&scrape_metrics(&app), "config_reload_success", &[]) == Some(0.0)
    });

    // A successful reload restores it to 1.
    std::fs::write(
        &config_path,
        support::config_yaml_for(
            &upstream.url(),
            app.data_plane_addr_port(),
            app.admin_addr_port(),
            &support::tempdir(),
            "",
        ),
    )
    .expect("restore the config file");
    let response = http_client()
        .post(format!("{}/reload", app.admin_url))
        .send()
        .expect("POST /reload");
    assert_eq!(response.status().as_u16(), 200);
    support::wait_until(|| {
        metric_value(&scrape_metrics(&app), "config_reload_success", &[]) == Some(1.0)
    });
}

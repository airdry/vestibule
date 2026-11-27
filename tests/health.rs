//! End-to-end tests for Upstream Health tracking: active probes, passive
//! delivery failure reporting, delivery pausing and buffering while
//! `Unhealthy`, automatic draining on recovery, and Admin Plane status
//! endpoints staying independent of `Upstream Health` (per ADR-0003).

mod support;

use std::time::{Duration, Instant};

use support::{MockUpstream, UpstreamResponse, http_client};

/// Ingests a valid envelope on the envelope route and asserts acceptance.
fn ingest_envelope(app: &support::TestApp, event_id: &str, key: &str) {
    let item = "{\"message\":\"boom\"}".to_string();
    let envelope = format!(
        "{{\"event_id\":\"{event_id}\"}}\n{{\"type\":\"event\",\"length\":{}}}\n{}\n",
        item.len(),
        item
    );
    let response = http_client()
        .post(format!(
            "{}/api/42/envelope/?sentry_key={key}",
            app.data_plane_url
        ))
        .header(
            "X-Sentry-Auth",
            format!("Sentry sentry_key={key}, sentry_version=7"),
        )
        .header("Content-Type", "application/x-sentry-envelope")
        .body(envelope)
        .send()
        .expect("ingestion request");
    assert_eq!(response.status().as_u16(), 201);
}

/// Delivery requests the mock `Upstream` received (POSTs on the envelope
/// route), excluding active health probes (GETs).
fn delivered_requests(upstream: &MockUpstream) -> usize {
    upstream
        .recorded_requests()
        .iter()
        .filter(|request| request.method == "POST")
        .count()
}

/// Polls the Admin Plane `GET /status` until it returns the wanted
/// `Upstream Health` value, then returns the last status document.
fn wait_for_health(app: &support::TestApp, health: &str) -> serde_json::Value {
    let mut last = get_status(app);
    support::wait_until(|| {
        last = get_status(app);
        last["upstream"]["health"] == health
    });
    last
}

/// Fetches the Admin Plane `GET /status` document.
fn get_status(app: &support::TestApp) -> serde_json::Value {
    let response = http_client()
        .get(format!("{}/status", app.admin_url))
        .send()
        .expect("GET /status on the Admin Plane");
    assert_eq!(response.status().as_u16(), 200);
    response.json().expect("decode /status JSON")
}

/// Builds a configuration with fast healthcheck settings.
fn health_yaml(
    upstream_url: &str,
    interval: &str,
    unhealthy_threshold: u32,
    healthy_threshold: u32,
) -> String {
    format!(
        "server:\n\
         \x20 data_plane_addr: \"127.0.0.1:{}\"\n\
         \x20 admin_addr: \"127.0.0.1:{}\"\n\
         upstream:\n\
         \x20 url: \"{upstream_url}\"\n\
         \x20 healthcheck:\n\
         \x20 \x20 interval: {interval}\n\
         \x20 \x20 timeout: 1s\n\
         \x20 \x20 unhealthy_threshold: {unhealthy_threshold}\n\
         \x20 \x20 healthy_threshold: {healthy_threshold}\n\
         queue:\n\
         \x20 dir: \"{}\"",
        support::ephemeral_port(),
        support::ephemeral_port(),
        support::tempdir().display()
    )
}

#[test]
fn readyz_stays_200_and_traffic_buffers_while_unhealthy() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("ignored"));
    let config = health_yaml(&upstream.url(), "50ms", 1, 1);
    let app = support::start_service(&config);

    // Baseline: one envelope flows through end to end.
    ingest_envelope(&app, "11111111111111111111111111111111", "key-a");
    support::wait_until(|| delivered_requests(&upstream) == 1);

    // Outage: everything the Upstream serves fails, probes included.
    upstream.set_response(UpstreamResponse::with_status(503));
    ingest_envelope(&app, "22222222222222222222222222222222", "key-a");
    ingest_envelope(&app, "33333333333333333333333333333333", "key-b");

    let status = wait_for_health(&app, "unhealthy");
    assert_eq!(status["config"]["status"], "loaded");

    // /readyz stays 200 OK during the outage (ADR-0003).
    let readyz = http_client()
        .get(format!("{}/readyz", app.admin_url))
        .send()
        .expect("GET /readyz on the Admin Plane");
    assert_eq!(readyz.status().as_u16(), 200);

    // The buffered records are visible in the queue utilization.
    assert!(
        status["queue"]["pending_bytes"].as_u64().unwrap_or(0) > 0,
        "undelivered records count as pending bytes: {status}"
    );
    assert!(
        status["queue"]["segments"].as_u64().unwrap_or(0) >= 1,
        "at least one segment file exists: {status}"
    );
    assert!(
        status["queue"]["oldest_entry_age_seconds"].is_number(),
        "the oldest buffered entry has an age: {status}"
    );

    // Delivery workers pause: no further delivery attempts arrive. Wait for
    // in-flight attempts to settle, then assert the pause over a quiet
    // window (nothing happening has no positive observable state).
    let mut paused_count = delivered_requests(&upstream);
    support::wait_until_within(Duration::from_secs(1), || {
        let latest = delivered_requests(&upstream);
        let settled = latest == paused_count;
        paused_count = latest;
        settled
    });
    support::assert_no_change_for(
        Duration::from_millis(300),
        || delivered_requests(&upstream) == paused_count,
        "no delivery attempts while Upstream Health is Unhealthy",
    );

    // Recovery: probes restore Healthy and the queue drains automatically.
    upstream.set_response(UpstreamResponse::ok("ignored"));
    support::wait_until(|| delivered_requests(&upstream) >= paused_count + 2);
    let recovered = wait_for_health(&app, "healthy");
    support::wait_until(|| get_status(&app)["queue"]["pending_bytes"].as_u64() == Some(0));
    assert_eq!(
        recovered["config"]["status"], "loaded",
        "the config stays loaded: {recovered}"
    );
}

#[test]
fn passive_delivery_failures_flip_health_before_the_next_probe() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("ignored"));
    // Probes are slow (2s) so only passive delivery failures can flip the
    // health within the assertion window.
    let config = health_yaml(&upstream.url(), "2s", 2, 1);
    let app = support::start_service(&config);

    ingest_envelope(&app, "11111111111111111111111111111111", "key-a");
    support::wait_until(|| delivered_requests(&upstream) == 1);

    upstream.set_response(UpstreamResponse::with_status(503));
    let outage_started = Instant::now();
    // Two distinct credentials produce two separate delivery jobs whose
    // failures are reported passively.
    ingest_envelope(&app, "22222222222222222222222222222222", "key-b");
    ingest_envelope(&app, "33333333333333333333333333333333", "key-c");

    wait_for_health(&app, "unhealthy");
    assert!(
        outage_started.elapsed() < Duration::from_secs(2),
        "passive failures flipped the health without waiting for a probe"
    );

    // Recovery: the next probe succeeds and workers resume draining.
    upstream.set_response(UpstreamResponse::ok("ignored"));
    support::wait_until(|| delivered_requests(&upstream) >= 3);
    wait_for_health(&app, "healthy");
}

#[test]
fn status_reports_a_drained_queue_and_loaded_config_when_healthy() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("ignored"));
    let config = health_yaml(&upstream.url(), "50ms", 1, 1);
    let app = support::start_service(&config);

    ingest_envelope(&app, "11111111111111111111111111111111", "key-a");
    support::wait_until(|| delivered_requests(&upstream) == 1);
    support::wait_until(|| get_status(&app)["queue"]["pending_bytes"].as_u64() == Some(0));

    let status = wait_for_health(&app, "healthy");
    assert_eq!(status["upstream"]["health"], "healthy");
    assert_eq!(status["queue"]["pending_bytes"], 0);
    assert!(
        status["queue"]["segments"].as_u64().unwrap_or(0) >= 1,
        "the drained queue still holds its segment file: {status}"
    );
    assert_eq!(
        status["queue"]["oldest_entry_age_seconds"],
        serde_json::Value::Null
    );
    assert_eq!(status["config"]["status"], "loaded");
}

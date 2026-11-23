//! End-to-end tests for asynchronous delivery: queued payloads are forwarded
//! to the `Upstream` by the delivery worker, with retry/backoff handling of
//! the documented error classes (per ADR-0002).

mod support;

use std::time::Duration;

use support::{MockUpstream, UpstreamResponse, http_client};

/// Spins up the service with the given upstream response plan and posts one
/// ingestion request.
fn start_delivery(upstream_responses: Vec<UpstreamResponse>) -> (support::TestApp, MockUpstream) {
    let upstream = MockUpstream::start(UpstreamResponse::ok("ignored"));
    upstream.set_responses(upstream_responses);
    let config = support::config_yaml_with(
        &upstream.url(),
        support::ephemeral_port(),
        support::ephemeral_port(),
        "",
    );
    let app = support::start_service(&config);
    (app, upstream)
}

/// Posts a single ingestion request through the Data Plane.
fn ingest(app: &support::TestApp, body: &str) {
    let response = http_client()
        .post(format!(
            "{}/api/42/envelope/?sentry_key=abc123",
            app.data_plane_url
        ))
        .header(
            "X-Sentry-Auth",
            "Sentry sentry_key=abc123, sentry_version=7",
        )
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .expect("ingestion request");
    assert_eq!(response.status().as_u16(), 201);
}

/// Polls until `condition` holds or a 10s deadline passes.
fn wait_until<F: Fn() -> bool>(condition: F) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        if condition() {
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    panic!("condition was not met within 10 seconds");
}

#[test]
fn queued_payload_is_delivered_to_upstream_with_metadata() {
    let (app, upstream) = start_delivery(vec![UpstreamResponse::ok("stored")]);

    ingest(&app, r#"{"event_id":"aaa"}"#);

    wait_until(|| !upstream.recorded_requests().is_empty());
    let forwarded = &upstream.recorded_requests()[0];
    assert_eq!(forwarded.method, "POST");
    assert_eq!(forwarded.path, "/api/42/envelope/?sentry_key=abc123");
    assert_eq!(
        forwarded.header("x-sentry-auth"),
        Some("Sentry sentry_key=abc123, sentry_version=7")
    );
    assert_eq!(forwarded.header("content-type"), Some("application/json"));
    assert_eq!(forwarded.body, br#"{"event_id":"aaa"}"#.to_vec());
}

#[test]
fn upstream_500_is_retried_with_backoff_until_success() {
    let (app, upstream) = start_delivery(vec![
        UpstreamResponse::with_status(500),
        UpstreamResponse::with_status(503),
        UpstreamResponse::ok("ok"),
    ]);

    ingest(&app, "payload");

    wait_until(|| upstream.recorded_requests().len() >= 3);
    let requests = upstream.recorded_requests();
    assert_eq!(
        requests.len(),
        3,
        "payload must not be redelivered after 2xx"
    );
    assert!(requests.iter().all(|r| r.body == b"payload".to_vec()));
}

#[test]
fn throttled_429_pauses_the_worker_for_retry_after() {
    let mut response = UpstreamResponse::with_status(429);
    response.headers = vec![("Retry-After".to_string(), "1".to_string())];
    let (app, upstream) = start_delivery(vec![response, UpstreamResponse::ok("ok")]);

    ingest(&app, "payload");

    wait_until(|| upstream.recorded_requests().len() >= 2);
    let requests = upstream.recorded_requests();
    assert_eq!(requests.len(), 2);
    let retry_gap = requests[1]
        .recorded_at
        .duration_since(requests[0].recorded_at);
    assert!(
        retry_gap >= Duration::from_millis(900),
        "delivery must pause for the requested Retry-After, gap was {retry_gap:?}"
    );
}

#[test]
fn unrecoverable_4xx_drops_the_record_and_does_not_block_the_queue() {
    let (app, upstream) = start_delivery(vec![
        UpstreamResponse::with_status(400),
        UpstreamResponse::ok("ok"),
    ]);

    ingest(&app, "bad payload");
    ingest(&app, "good payload");

    wait_until(|| upstream.recorded_requests().len() >= 2);
    let requests = upstream.recorded_requests();
    assert_eq!(
        requests.len(),
        2,
        "the rejected record must be dropped after one attempt"
    );
    assert_eq!(requests[0].body, b"bad payload".to_vec());
    assert_eq!(requests[1].body, b"good payload".to_vec());
}

#[test]
fn upstream_timeout_is_retried_and_delivered_on_the_next_attempt() {
    let upstream = MockUpstream::start(
        UpstreamResponse::with_status(200).with_latency(Duration::from_millis(800)),
    );
    upstream.set_responses(vec![
        UpstreamResponse::with_status(200).with_latency(Duration::from_millis(800)),
        UpstreamResponse::ok("ok"),
    ]);
    let config = format!(
        "server:\n\
         \x20 data_plane_addr: \"127.0.0.1:{}\"\n\
         \x20 admin_addr: \"127.0.0.1:{}\"\n\
         upstream:\n\
         \x20 url: \"{}\"\n\
         \x20 timeout: 200ms\n\
         queue:\n\
         \x20 dir: \"{}\"\n",
        support::ephemeral_port(),
        support::ephemeral_port(),
        upstream.url(),
        support::tempdir().display(),
    );
    let app = support::start_service(&config);

    ingest(&app, "payload");

    wait_until(|| upstream.recorded_requests().len() >= 2);
    let requests = upstream.recorded_requests();
    assert!(requests.iter().any(|r| r.body == b"payload".to_vec()));
}

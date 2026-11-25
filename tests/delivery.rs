//! End-to-end tests for asynchronous delivery: queued payloads are forwarded
//! to the `Upstream` by the delivery worker, with retry/backoff handling of
//! the documented error classes (per ADR-0002).

mod support;

use std::time::Duration;

use support::{MockUpstream, UpstreamResponse, http_client};

use std::io::Read as _;

/// Spins up the service with the given response plan for delivery requests
/// on the envelope route. Active health probes always receive `200`, so
/// response sequences stay deterministic.
fn start_delivery(upstream_responses: Vec<UpstreamResponse>) -> (support::TestApp, MockUpstream) {
    let upstream = MockUpstream::start(UpstreamResponse::ok("ignored"));
    upstream.set_responses_for_path("/api/42/envelope/", upstream_responses);
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

/// Ingests a valid envelope item on the envelope route.
fn ingest_envelope(app: &support::TestApp, message: &str, key: &str) {
    let item = format!("{{\"message\":\"{message}\"}}");
    let envelope = format!(
        "{{\"event_id\":\"{}\"}}\n{{\"type\":\"event\",\"length\":{}}}\n{}\n",
        "11111111111111111111111111111111",
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

    wait_until(|| !support::non_probe_requests(&upstream).is_empty());
    let forwarded = &support::non_probe_requests(&upstream)[0];
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

    wait_until(|| support::non_probe_requests(&upstream).len() >= 3);
    let requests = support::non_probe_requests(&upstream);
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

    wait_until(|| support::non_probe_requests(&upstream).len() >= 2);
    let requests = support::non_probe_requests(&upstream);
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

    wait_until(|| support::non_probe_requests(&upstream).len() >= 2);
    let requests = support::non_probe_requests(&upstream);
    assert_eq!(
        requests.len(),
        2,
        "the rejected record must be dropped after one attempt"
    );
    let mut delivered: Vec<&[u8]> = requests
        .iter()
        .map(|request| request.body.as_slice())
        .collect();
    delivered.sort();
    assert_eq!(
        delivered,
        [b"bad payload".as_slice(), b"good payload".as_slice()]
    );
}

#[test]
fn upstream_timeout_is_retried_and_delivered_on_the_next_attempt() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("ignored"));
    upstream.set_responses_for_path(
        "/api/42/envelope/",
        vec![
            UpstreamResponse::with_status(200).with_latency(Duration::from_millis(800)),
            UpstreamResponse::ok("ok"),
        ],
    );
    let config = format!(
        "server:\n\
         \x20 data_plane_addr: \"127.0.0.1:{}\"\n\
         \x20 admin_addr: \"127.0.0.1:{}\"\n\
         upstream:\n\
         \x20 url: \"{}\"\n\
         \x20 timeout: 200ms\n\
         \x20 healthcheck:\n\
         \x20 \x20 path: \"{}\"\n\
         queue:\n\
         \x20 dir: \"{}\"\n",
        support::ephemeral_port(),
        support::ephemeral_port(),
        upstream.url(),
        support::PROBE_PATH,
        support::tempdir().display(),
    );
    let app = support::start_service(&config);

    ingest(&app, "payload");

    wait_until(|| support::non_probe_requests(&upstream).len() >= 2);
    let requests = support::non_probe_requests(&upstream);
    assert!(requests.iter().any(|r| r.body == b"payload".to_vec()));
}

#[test]
fn compatible_envelopes_are_merged_into_one_gzip_request() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("ignored"));
    let config = support::config_yaml_with(
        &upstream.url(),
        support::ephemeral_port(),
        support::ephemeral_port(),
        "batch:\n  max_items: 2\n",
    );
    let app = support::start_service(&config);

    ingest_envelope(&app, "first", "abc123");
    ingest_envelope(&app, "second", "abc123");

    wait_until(|| !support::non_probe_requests(&upstream).is_empty());
    // Give the second record a chance to join the same batch.
    std::thread::sleep(Duration::from_millis(300));
    let requests = support::non_probe_requests(&upstream);
    assert_eq!(
        requests.len(),
        1,
        "both records must merge into one request"
    );
    let request = &requests[0];
    assert_eq!(request.path, "/api/42/envelope/?sentry_key=abc123");
    assert_eq!(
        request.header("content-type"),
        Some("application/x-sentry-envelope")
    );
    assert_eq!(request.header("content-encoding"), Some("gzip"));

    let mut decoder = flate2::read::GzDecoder::new(&request.body[..]);
    let mut body = Vec::new();
    decoder
        .read_to_end(&mut body)
        .expect("gunzip merged envelope");
    let text = String::from_utf8(body).expect("envelope is utf8");
    assert!(text.contains("\"first\""), "merged body: {text}");
    assert!(text.contains("\"second\""), "merged body: {text}");
}

#[test]
fn different_sentry_keys_are_never_merged() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("ignored"));
    let config = support::config_yaml_with(
        &upstream.url(),
        support::ephemeral_port(),
        support::ephemeral_port(),
        "batch:\n  max_items: 4\n",
    );
    let app = support::start_service(&config);

    ingest_envelope(&app, "first", "abc123");
    ingest_envelope(&app, "second", "def456");

    wait_until(|| support::non_probe_requests(&upstream).len() >= 2);
    let requests = support::non_probe_requests(&upstream);
    assert_eq!(
        requests.len(),
        2,
        "records with different keys must not merge"
    );
    let keys: Vec<&str> = requests
        .iter()
        .map(|request| request.path.split("sentry_key=").nth(1))
        .collect::<Option<Vec<&str>>>()
        .expect("recorded requests carry the sentry_key parameter");
    let mut keys = keys;
    keys.sort();
    assert_eq!(keys, ["abc123", "def456"]);
}

#[test]
fn a_single_envelope_record_is_flushed_after_flush_interval() {
    let upstream = MockUpstream::start(UpstreamResponse::ok("ignored"));
    let config = support::config_yaml_with(
        &upstream.url(),
        support::ephemeral_port(),
        support::ephemeral_port(),
        "batch:\n  max_items: 100\n  flush_interval: 50ms\n",
    );
    let app = support::start_service(&config);

    ingest_envelope(&app, "single", "abc123");

    wait_until(|| !support::non_probe_requests(&upstream).is_empty());
    let requests = support::non_probe_requests(&upstream);
    assert_eq!(requests.len(), 1, "exactly one flush must happen");
    let request = &requests[0];
    assert_eq!(request.path, "/api/42/envelope/?sentry_key=abc123");
    assert!(
        request.header("content-encoding").is_none(),
        "a single-record batch is forwarded verbatim"
    );
    assert!(String::from_utf8_lossy(&request.body).contains("\"single\""));
}
